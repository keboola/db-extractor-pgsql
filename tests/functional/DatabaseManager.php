<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DbExtractor\TraitTests\Tables\AutoIncrementTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\EmojiTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\EscapingTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SalesTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\SimpleTableTrait;
use Keboola\DbExtractor\TraitTests\Tables\TypesTableTrait;
use PDO;

class DatabaseManager
{
    use SimpleTableTrait;
    use AutoIncrementTableTrait;
    use SalesTableTrait;
    use EscapingTableTrait;
    use EmojiTableTrait;
    use TypesTableTrait;

    protected PDO $connection;

    public function __construct(PDO $connection)
    {
        $this->connection = $connection;
    }
}
