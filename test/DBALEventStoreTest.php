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

namespace Broadway\EventStore\Dbal;

use Broadway\EventStore\Testing\EventStoreTest;
use Broadway\Serializer\SimpleInterfaceSerializer;
use Broadway\UuidGenerator\Converter\BinaryUuidConverter;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * @requires extension pdo_sqlite
 */
class DBALEventStoreTest extends EventStoreTest
{
    use ProphecyTrait;

    protected function setUp(): void
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $schemaManager = $connection->getSchemaManager();
        $schema = $schemaManager->createSchema();
        $this->eventStore = new DBALEventStore(
            $connection,
            new SimpleInterfaceSerializer(),
            new SimpleInterfaceSerializer(),
            'events',
            false,
            new BinaryUuidConverter()
        );

        $table = $this->eventStore->configureSchema($schema);
        $schemaManager->createTable($table);
    }

    /**
     * @test
     *
     * @doesNotPerformAssertions
     */
    public function it_allows_no_binary_uuid_converter_provided_when_not_using_binary()
    {
        $eventStore = new DBALEventStore(
            $this->prophesize(Connection::class)->reveal(),
            new SimpleInterfaceSerializer(),
            new SimpleInterfaceSerializer(),
            'events',
            false,
            null
        );
    }
}
