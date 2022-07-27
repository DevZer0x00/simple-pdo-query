<?php

declare(strict_types=1);

namespace DevZer0x00\SimplePDOQuery\ResultTransformer;

class OneDimensionalTransformer implements ResultTransformerInterface
{
    private string $key;

    public function __construct(string $key)
    {
        $this->key = $key;
    }

    public function transform(array $data): array
    {
        $key = $this->key;

        $result = [];

        foreach ($data as $row) {
            $result[$row[$key]] = $row;
        }

        return $result;
    }
}
