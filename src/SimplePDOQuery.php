<?php

namespace DevZer0x00\SimplePDOQuery;

use DevZer0x00\SimplePDOQuery\Exception\InvalidArgumentException;
use DevZer0x00\SimplePDOQuery\ResultTransformer\ResultTransformerInterface;
use PDO;
use PDOStatement;

class SimplePDOQuery
{
    const TRANSFORM_ARRAY_KEY = 'array_key';

    const OPTIONAL_SKIP = INF;

    /**
     * @var PDO
     */
    private $pdo;
 
    public function __construct($pdo)
    {
        $this->pdo = $pdo;
    }

    public function transaction()
    {
        return $this->pdo->beginTransaction();
    }

    public function commit()
    {
        return $this->pdo->commit();
    }

    public function rollback()
    {
        return $this->pdo->rollBack();
    }

    public function query(string $query, $parameters = []): int
    {
        $st = $this->doQuery($query, $parameters);

        if (preg_match('/^\s* INSERT \s+/six', $query)) {
            return $this->pdo->lastInsertId();
        }

        if ($st->columnCount() == 0) {
            return $st->rowCount();
        }

        return 0;
    }

    public function select(string $query, $parameters = [], ResultTransformerInterface $transformer = null): array
    {
        $st = $this->doQuery($query, $parameters);

        if ($transformer === null) {
            return $st->fetchAll(PDO::FETCH_ASSOC);
        }

        $data = $st->fetchAll(PDO::FETCH_ASSOC);
        $data = $data ? $data : [];

        return $transformer->transform($data);
    }

    public function selectRow(string $query, $parameters = []): array
    {
        $st = $this->doQuery($query, $parameters);

        $result = $st->fetch(PDO::FETCH_ASSOC);

        $st->closeCursor();

        return $result ? $result : [];
    }

    public function selectCol(string $query, $parameters = [], $fetchKeyPair = false): array
    {
        $st = $this->doQuery($query, $parameters);

        if ($fetchKeyPair) {
            $result = $st->fetchAll(PDO::FETCH_KEY_PAIR);
        } else {
            $result = $st->fetchAll(PDO::FETCH_COLUMN);
        }

        $st->closeCursor();

        return $result ? $result : [];
    }

    public function selectCell(string $query, $parameters = [])
    {
        $row = $this->selectCol($query, $parameters);

        return current($row);
    }

    public function escape($s)
    {
        if (is_int($s)) {
            return $s;
        } elseif (is_float($s)) {
            return str_replace(',', '.', $s);
        }

        return $this->pdo->quote($s);
    }

    private function doQuery($query, $parameters): PDOStatement
    {
        $parameters = array_reverse($parameters);
        $query = $this->expandPlaceholders($query, $parameters);

        return $this->pdo->query($query);
    }

    private function expandPlaceholders($query, &$parameters, &$paramIndex = 0)
    {
        $re = '{
            (?>
                # Ignored chunks.
                (?>
                    # Comment.
                    -- [^\r\n]*
                )
                  |
                (?>
                    # DB-specifics.
                    "   (?> [^"\\\\]+|\\\\"|\\\\)*    "   |
                    \'  (?> [^\'\\\\]+|\\\\\'|\\\\)* \'   |
                    `   (?> [^`]+ | ``)*              `   |   # backticks
                    /\* .*?                          \*/      # comments
                )
            )
              |
            (?>
                # Optional blocks
                \{
                    # Use "+" here, not "*"! Else nested blocks are not processed well.
                    ( (?> (?>(\??)[^{}]+)  |  (?R) )* )             #1
                \}
            )
              |
            (?>
                # Placeholder
                (\?) ( [_dsafn&|\#]? )                           #2 #3
            )
        }sxS';

        $query = preg_replace_callback(
            $re,
            function($m) use (&$parameters, &$paramIndex) {
                return $this->expandPlaceholdersCallback($m, $parameters, $paramIndex);
            },
            $query
        );

        return $query;
    }

    private function expandPlaceholdersCallback($m, &$parameters, &$paramIndex)
    {
        // Placeholder.
        if (!empty($m[3])) {
            $type = $m[4];

            $value = array_pop($parameters);
            $paramIndex++;

            // Skip this value?
            if ($value === self::OPTIONAL_SKIP) {
                return '';
            }

            // First process guaranteed non-native placeholders.
            switch ($type) {
                case 's':
                    return $this->expandPlaceholders($value, $parameters, $paramIndex);
                case 'a':
                    if (!is_array($value)) {
                        throw new InvalidArgumentException('Placeholder value is not array - param %', $paramIndex);
                    }

                    $parts = $multi = [];
                    $mult = is_int(key($value)) && is_array(current($value));

                    foreach ($value as $prefix => $field) {
                        if (!is_array($field)) {
                            $field = [$prefix => $field];
                            $prefix = 0;
                        }

                        $prefix = is_int($prefix) ? '' : $this->escape($prefix) . '.';
                        //для мультиинсерта очищаем ключи - их быть не может по синтаксису

                        if ($mult && $type == 'a') {
                            $field = array_values($field);
                        }

                        foreach ($field as $k => $v) {
                            $v = $v === null ? 'NULL' : $this->escape($v);

                            if (!is_int($k)) {
                                $k = $this->escape($k);
                                $parts[] = "$prefix$k=$v";
                            } else {
                                $parts[] = $v;
                            }
                        }
                        if ($mult) {
                            $multi[] = join(',', $parts);
                            $parts = [];
                        }
                    }

                    return $mult
                        ? join('), (', $multi)
                        : join(', ', $parts);
                case '#':
                    // Identifier.
                    if (!is_array($value)) {
                        return $this->escape($value);
                    }

                    $parts = array();
                    foreach ($value as $table => $identifiers) {
                        if (!is_array($identifiers)) {
                            $identifiers = array($identifiers);
                        }
                        $prefix = '';
                        if (!is_int($table)) {
                            $prefix = $this->escape($table) . '.';
                        }
                        foreach ($identifiers as $identifier) {
                            if (!is_string($identifier)) {
                                throw new InvalidArgumentException('Placeholder value is not string - param %', $paramIndex);
                            } else {
                                $parts[] = $prefix . ($identifier == '*' ? '*' :
                                        $this->escape($identifier));
                            }
                        }
                    }

                    return join(', ', $parts);
                case 'n':
                    // NULL-based placeholder.
                    return empty($value) ? 'NULL' : intval($value);
            }

            // In non-native mode arguments are quoted.
            if ($value === null) {
                return 'NULL';
            }
            switch ($type) {
                case '':
                    if (!is_scalar($value)) {
                        throw new InvalidArgumentException('Placeholder value is not scalar - param %', $paramIndex);
                    }

                    return $this->escape($value);
                case 'd':
                    return intval($value);
                case 'f':
                    return str_replace(',', '.', floatval($value));
            }

            // By default - escape as string.
            return $this->escape($value);
        }

        // Optional block.
        if (isset($m[1]) && strlen($block = $m[1])) {
            // Проверка на {?  } - условный блок
            $skip = false;
            if ($m[2] == '?') {
                $skip = array_pop($parameters) === self::OPTIONAL_SKIP;
                $block[0] = ' ';
            }

            $block = $this->expandOptionalBlock($block, $parameters, $paramIndex);

            if ($skip) {
                $block = '';
            }
            
            return $block;
        }

        // Default: skipped part of the string.
        return $m[0];
    }

    /**
     * Разбирает опциональный блок - условие |
     *
     * @param string $block блок, который нужно разобрать
     * @return string что получается в результате разбора блока
     */
    private function expandOptionalBlock($block, &$parameters, &$paramIndex)
    {
        $alts = array();
        $alt = '';
        $sub = 0;
        $exp = explode('|', $block);
        // Оптимизация, так как в большинстве случаев | не используется
        if (count($exp) == 1) {
            $alts = $exp;
        } else {
            foreach ($exp as $v) {
                // Реализуем автоматный магазин для нахождения нужной скобки
                // На суммарную парность скобок проверять нет необходимости - об этом заботится регулярка
                $sub += substr_count($v, '{');
                $sub -= substr_count($v, '}');
                if ($sub > 0) {
                    $alt .= $v . '|';
                } else {
                    $alts[] = $alt . $v;
                    $alt = '';
                }
            }
        }
        $r = '';
        foreach ($alts as $block) {
            $plNoValue = false;

            $block = $this->expandPlaceholders($block, $parameters, $paramIndex);
            // Необходимо пройти все блоки, так как если пропустить оставшиесь,
            // то это нарушит порядок подставляемых значений
            if ($plNoValue == false && $r == '') {
                $r = ' ' . $block . ' ';
            }
        }

        return $r;
    }
}