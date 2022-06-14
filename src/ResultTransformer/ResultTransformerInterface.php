<?php

declare(strict_types=1);

namespace DevZer0x00\SimplePDOQuery\ResultTransformer;

interface ResultTransformerInterface
{
    public function transform(array $data): array;
}
