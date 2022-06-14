<?php

declare(strict_types=1);

namespace DevZer0x00\SimplePDOQuery\ResultTransformer;

use function count;

class TwoDimensionalTransformer implements ResultTransformerInterface
{
    private array $keys;

    public function __construct(array $keys)
    {
        $this->keys = $keys;
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
            $result[$row[$key]] = $row;
        }

        return $result;
    }

    private function transformTree(array $data): array
    {
        $result = [];
        $keys = array_reverse($this->keys);

        foreach ($data as $row) {
            $m = $row;

            foreach ($keys as $key) {
                $m = [$row[$key] => $m];
            }

            $result = array_merge_recursive($result, $m);
        }

        return $result;
    }
}
