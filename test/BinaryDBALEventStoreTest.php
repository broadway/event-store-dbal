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

use Broadway\Domain\DomainEventStream;
use Broadway\Serializer\SimpleInterfaceSerializer;
use Broadway\UuidGenerator\Converter\BinaryUuidConverter;
use Broadway\UuidGenerator\Rfc4122\Version4Generator;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * @requires extension pdo_sqlite
 */
class BinaryDBALEventStoreTest extends DBALEventStoreTest
{
    use ProphecyTrait;

    /** @var \Doctrine\DBAL\Schema\Table */
    protected $table;

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
            true,
            new BinaryUuidConverter()
        );

        $this->table = $this->eventStore->configureSchema($schema);

        $schemaManager->createTable($this->table);
    }

    /**
     * @test
     */
    public function table_should_contain_binary_uuid_column()
    {
        $uuidColumn = $this->table->getColumn('uuid');

        $this->assertEquals(16, $uuidColumn->getLength());
        $this->assertEquals(Type::getType(Types::BINARY), $uuidColumn->getType());
        $this->assertTrue($uuidColumn->getFixed());
    }

    /**
     * @test
     */
    public function it_throws_an_exception_when_an_id_is_no_uuid_in_binary_mode()
    {
        $this->expectException('Broadway\EventStore\Exception\InvalidIdentifierException');
        $this->expectExceptionMessage('Only valid UUIDs are allowed to by used with the binary storage mode.');
        $id = 'bleeh';
        $domainEventStream = new DomainEventStream([
            $this->createDomainMessage($id, 0),
        ]);

        $this->eventStore->append($id, $domainEventStream);
    }

    public function idDataProvider()
    {
        return [
            'UUID String' => [
                (new Version4Generator())->generate(), // test UUID
            ],
        ];
    }

    /**
     * @test
     */
    public function it_throws_when_no_binary_uuid_converter_provided_when_using_binary()
    {
        $this->expectException('LogicException');
        $this->expectExceptionMessage('binary UUID converter is required when using binary');
        $eventStore = new DBALEventStore(
            $this->prophesize(Connection::class)->reveal(),
            new SimpleInterfaceSerializer(),
            new SimpleInterfaceSerializer(),
            'events',
            true,
            null
        );
    }

    /**
     * Overriding base test as it doesn't use the data provider andthis testcase fails on non-uuid ids.
     *
     * @test
     */
    public function empty_set_of_events_can_be_added(): void
    {
        $id = (new Version4Generator())->generate();

        $domainMessage = $this->createDomainMessage($id, 0);
        $baseStream = new DomainEventStream([$domainMessage]);
        $this->eventStore->append($id, $baseStream);
        $appendedEventStream = new DomainEventStream([]);

        $this->eventStore->append($id, $appendedEventStream);

        $events = $this->eventStore->load($id);
        $this->assertCount(1, $events);
    }
}
