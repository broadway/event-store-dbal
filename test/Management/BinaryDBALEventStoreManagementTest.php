<?php

declare(strict_types=1);

/*
 * This file is part of the broadway/event-store-dbal package.
 *
 * (c) 2020 Broadway project
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Broadway\EventStore\Dbal\Management;

use Broadway\EventStore\Dbal\DBALEventStore;
use Broadway\Serializer\SimpleInterfaceSerializer;
use Broadway\UuidGenerator\Converter\BinaryUuidConverter;
use Doctrine\DBAL\DriverManager;

/**
 * @requires extension pdo_sqlite
 */
class BinaryDBALEventStoreManagementTest extends DBALEventStoreManagementTest
{
    /** @var \Doctrine\DBAL\Schema\Table */
    protected $table;

    public function createEventStore()
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $schemaManager = $connection->getSchemaManager();
        $schema = $schemaManager->createSchema();
        $eventStore = new DBALEventStore(
            $connection,
            new SimpleInterfaceSerializer(),
            new SimpleInterfaceSerializer(),
            'events',
            true,
            new BinaryUuidConverter()
        );

        $this->table = $eventStore->configureSchema($schema);

        $schemaManager->createTable($this->table);

        return $eventStore;
    }
}
