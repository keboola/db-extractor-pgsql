<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests\Tables;

use Keboola\DbExtractor\TraitTests\AddConstraintTrait;
use Keboola\DbExtractor\TraitTests\CreateTableTrait;
use Keboola\DbExtractor\TraitTests\InsertRowsTrait;

trait EmojiTableTrait
{
    use CreateTableTrait;
    use InsertRowsTrait;
    use AddConstraintTrait;

    public function createEmojiTable(string $name = 'emoji'): void
    {
        $this->createTable($name, $this->getEmojiColumns());
    }

    public function generateEmojiRows(string $tableName = 'emoji'): void
    {
        $data = $this->getEmojiRows();
        $this->insertRows($tableName, $data['columns'], $data['data']);
    }

    private function getEmojiRows(): array
    {
        return [
            'columns' => ['emoji'],
            'data' => [
                ['😁'],
                ['😂'],
                ['😃'],
                ['😄'],
                ['😅'],
                ['😆'],
                ['😉'],
                ['😊'],
                ['😋'],
                ['😌'],
                ['😍'],
                ['😏'],
                ['😒'],
                ['😓'],
                ['😔'],
                ['😖'],
                ['😘'],
                ['😚'],
                ['😜'],
                ['😝'],
                ['😞'],
                ['😠'],
                ['😡'],
                ['😢'],
                ['😣'],
                ['😤'],
                ['😥'],
                ['😨'],
                ['😩'],
                ['😪'],
                ['😫'],
                ['😭'],
                ['😰'],
                ['😱'],
                ['😲'],
                ['😳'],
                ['😵'],
                ['😷'],
                ['😸'],
                ['😹'],
                ['😺'],
                ['😻'],
                ['😼'],
                ['😽'],
                ['😾'],
                ['😿'],
                ['🙀'],
                ['🙅'],
                ['🙆'],
                ['🙇'],
                ['🙈'],
                ['🙉'],
                ['🙊'],
                ['🙋'],
                ['🙌'],
                ['🙍'],
                ['🙎'],
                ['🙏'],
            ],
        ];
    }

    private function getEmojiColumns(): array
    {
        return [
            'emoji' => 'text CHARACTER SET utf8mb4 NULL',
        ];
    }
}
