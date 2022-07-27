<?php

declare(strict_types=1);

namespace DevZer0x00\SimplePDOQuery\ResultTransformer;

use function count;

class TwoDimensionalTransformer implements ResultTransformerInterface
{
    private array $keys;

    private bool $dataAsLastElement;

    public function __construct(array $keys, $dataAsLastElement = false)
    {
        $this->keys = $keys;
        $this->dataAsLastElement = $dataAsLastElement;
    }

    public function transform(array $data): array
    {
        if (count($this->keys) === 1) {
            return $this->transformOneKey($data);
        }

        return $this->transformTree($data);
    }

    private function transformOneKey(array $data): array
    {
        $key = reset($this->keys);

        $result = [];

        foreach ($data as $row) {
            $result[$row[$key]][] = $row;
        }

        return $result;
    }

    private function transformTree(array $data): array
    {
        $result = [];
        $keys = $this->keys;

        foreach ($data as $row) {
            $z = &$result;

            foreach ($keys as $key) {
                $z[$row[$key]] ??= [];
                $z = &$z[$row[$key]];
            }

            if ($this->dataAsLastElement) {
                $z = $row;
            } else {
                $z[] = $row;
            }
        }

        return $result;
    }
}
