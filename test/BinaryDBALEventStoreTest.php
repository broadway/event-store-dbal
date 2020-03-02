<?php

/*
 * This file is part of the broadway/broadway package.
 *
 * (c) Qandidate.com <opensource@qandidate.com>
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
use Doctrine\DBAL\Version;

/**
 * @requires extension pdo_sqlite
 */
class BinaryDBALEventStoreTest extends DBALEventStoreTest
{
    /** @var \Doctrine\DBAL\Schema\Table  */
    protected $table;

    protected function setUp(): void
    {
        if (Version::compare('2.5.0') >= 0) {
            $this->markTestSkipped('Binary type is only available for Doctrine >= v2.5');
        }

        $connection       = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $schemaManager    = $connection->getSchemaManager();
        $schema           = $schemaManager->createSchema();
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
        $this->assertEquals(Type::getType(Type::BINARY), $uuidColumn->getType());
        $this->assertTrue($uuidColumn->getFixed());
    }

    /**
     * @test
     */
    public function it_throws_an_exception_when_an_id_is_no_uuid_in_binary_mode()
    {
        $this->expectException('Broadway\EventStore\Exception\InvalidIdentifierException');
        $this->expectExceptionMessage('Only valid UUIDs are allowed to by used with the binary storage mode.');
        $id                = 'bleeh';
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
}
