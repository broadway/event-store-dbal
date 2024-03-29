<?php

/*
 * This file is part of the broadway/event-store-dbal package.
 *
 * (c) 2020 Broadway project
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Broadway\EventStore\Dbal;

use Broadway\Domain\DateTime;
use Broadway\Domain\DomainEventStream;
use Broadway\Domain\DomainMessage;
use Broadway\EventStore\EventStore;
use Broadway\EventStore\EventStreamNotFoundException;
use Broadway\EventStore\EventVisitor;
use Broadway\EventStore\Exception\DuplicatePlayheadException;
use Broadway\EventStore\Exception\InvalidIdentifierException;
use Broadway\EventStore\Management\Criteria;
use Broadway\EventStore\Management\CriteriaNotSupportedException;
use Broadway\EventStore\Management\EventStoreManagement;
use Broadway\Serializer\Serializer;
use Broadway\UuidGenerator\Converter\BinaryUuidConverterInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Statement;

/**
 * Event store using a relational database as storage.
 *
 * The implementation uses doctrine DBAL for the communication with the
 * underlying data store.
 */
class DBALEventStore implements EventStore, EventStoreManagement
{
    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var Serializer
     */
    private $payloadSerializer;

    /**
     * @var Serializer
     */
    private $metadataSerializer;

    /**
     * @var Statement|null
     */
    private $loadStatement;

    /**
     * @var string
     */
    private $tableName;

    /**
     * @var bool
     */
    private $useBinary;

    /**
     * @var BinaryUuidConverterInterface|null
     */
    private $binaryUuidConverter;

    public function __construct(
        Connection $connection,
        Serializer $payloadSerializer,
        Serializer $metadataSerializer,
        string $tableName,
        bool $useBinary,
        ?BinaryUuidConverterInterface $binaryUuidConverter = null
    ) {
        $this->connection = $connection;
        $this->payloadSerializer = $payloadSerializer;
        $this->metadataSerializer = $metadataSerializer;
        $this->tableName = $tableName;
        $this->useBinary = $useBinary;
        $this->binaryUuidConverter = $binaryUuidConverter;

        if ($this->useBinary && null === $binaryUuidConverter) {
            throw new \LogicException('binary UUID converter is required when using binary');
        }
    }

    public function load($id): DomainEventStream
    {
        $statement = $this->prepareLoadStatement();
        $statement->bindValue(1, $this->convertIdentifierToStorageValue($id));
        $statement->bindValue(2, 0);
        $result = $statement->executeQuery();

        $events = [];
        while ($row = $result->fetchAssociative()) {
            $events[] = $this->deserializeEvent($row);
        }

        if (empty($events)) {
            throw new EventStreamNotFoundException(sprintf('EventStream not found for aggregate with id %s for table %s', $id, $this->tableName));
        }

        return new DomainEventStream($events);
    }

    public function loadFromPlayhead($id, int $playhead): DomainEventStream
    {
        $statement = $this->prepareLoadStatement();
        $statement->bindValue(1, $this->convertIdentifierToStorageValue($id));
        $statement->bindValue(2, $playhead);
        $result = $statement->executeQuery();

        $events = [];
        while ($row = $result->fetchAssociative()) {
            $events[] = $this->deserializeEvent($row);
        }

        return new DomainEventStream($events);
    }

    public function append($id, DomainEventStream $eventStream): void
    {
        // noop to ensure that an error will be thrown early if the ID
        // is not something that can be converted to a string. If we
        // let this move on without doing this DBAL will eventually
        // give us a hard time but the true reason for the problem
        // will be obfuscated.
        $id = (string) $id;

        $this->connection->beginTransaction();

        try {
            /** @var DomainMessage $domainMessage */
            foreach ($eventStream as $domainMessage) {
                $this->insertMessage($this->connection, $domainMessage);
            }

            $this->connection->commit();
        } catch (UniqueConstraintViolationException $exception) {
            $this->connection->rollBack();

            throw new DuplicatePlayheadException($eventStream, $exception);
        } catch (Exception $exception) {
            $this->connection->rollBack();

            throw DBALEventStoreException::create($exception);
        }
    }

    private function insertMessage(Connection $connection, DomainMessage $domainMessage): void
    {
        $data = [
            'uuid' => $this->convertIdentifierToStorageValue((string) $domainMessage->getId()),
            'playhead' => $domainMessage->getPlayhead(),
            'metadata' => json_encode($this->metadataSerializer->serialize($domainMessage->getMetadata())),
            'payload' => json_encode($this->payloadSerializer->serialize($domainMessage->getPayload())),
            'recorded_on' => $domainMessage->getRecordedOn()->toString(),
            'type' => $domainMessage->getType(),
        ];

        $connection->insert($this->tableName, $data);
    }

    public function configureSchema(Schema $schema): ?\Doctrine\DBAL\Schema\Table
    {
        if ($schema->hasTable($this->tableName)) {
            return null;
        }

        return $this->configureTable($schema);
    }

    public function configureTable(?Schema $schema = null): \Doctrine\DBAL\Schema\Table
    {
        $schema = $schema ?: new Schema();

        $uuidColumnDefinition = [
            'type' => 'guid',
            'params' => [
                'length' => 36,
            ],
        ];

        if ($this->useBinary) {
            $uuidColumnDefinition['type'] = 'binary';
            $uuidColumnDefinition['params'] = [
                'length' => 16,
                'fixed' => true,
            ];
        }

        $table = $schema->createTable($this->tableName);

        $table->addColumn('id', 'integer', ['autoincrement' => true]);
        $table->addColumn('uuid', $uuidColumnDefinition['type'], $uuidColumnDefinition['params']);
        $table->addColumn('playhead', 'integer', ['unsigned' => true]);
        $table->addColumn('payload', 'text');
        $table->addColumn('metadata', 'text');
        $table->addColumn('recorded_on', 'string', ['length' => 32]);
        $table->addColumn('type', 'string', ['length' => 255]);

        $table->setPrimaryKey(['id']);
        $table->addUniqueIndex(['uuid', 'playhead']);

        return $table;
    }

    private function prepareLoadStatement(): Statement
    {
        if (null === $this->loadStatement) {
            $query = 'SELECT uuid, playhead, metadata, payload, recorded_on
                FROM '.$this->tableName.'
                WHERE uuid = ?
                AND playhead >= ?
                ORDER BY playhead ASC';
            $this->loadStatement = $this->connection->prepare($query);
        }

        return $this->loadStatement;
    }

    private function deserializeEvent(array $row): DomainMessage
    {
        return new DomainMessage(
            $this->convertStorageValueToIdentifier($row['uuid']),
            (int) $row['playhead'],
            $this->metadataSerializer->deserialize(json_decode($row['metadata'], true)),
            $this->payloadSerializer->deserialize(json_decode($row['payload'], true)),
            DateTime::fromString($row['recorded_on'])
        );
    }

    private function convertIdentifierToStorageValue(mixed $id): string
    {
        if ($this->useBinary) {
            try {
                return $this->binaryUuidConverter::fromString((string) $id);
            } catch (\Exception $e) {
                throw new InvalidIdentifierException('Only valid UUIDs are allowed to by used with the binary storage mode.');
            }
        }

        return (string) $id;
    }

    private function convertStorageValueToIdentifier(string $id): string
    {
        if ($this->useBinary) {
            try {
                return $this->binaryUuidConverter::fromBytes((string) $id);
            } catch (\Exception $e) {
                throw new InvalidIdentifierException('Could not convert binary storage value to UUID.');
            }
        }

        return $id;
    }

    public function visitEvents(Criteria $criteria, EventVisitor $eventVisitor): void
    {
        $result = $this->prepareVisitEventsStatement($criteria);

        while ($row = $result->fetchAssociative()) {
            $domainMessage = $this->deserializeEvent($row);

            $eventVisitor->doWithEvent($domainMessage);
        }
    }

    private function prepareVisitEventsStatement(Criteria $criteria): Result
    {
        list($where, $bindValues, $bindValueTypes) = $this->prepareVisitEventsStatementWhereAndBindValues($criteria);
        $query = 'SELECT uuid, playhead, metadata, payload, recorded_on
            FROM '.$this->tableName.'
            '.$where.'
            ORDER BY id ASC';

        return $this->connection->executeQuery($query, $bindValues, $bindValueTypes);
    }

    private function prepareVisitEventsStatementWhereAndBindValues(Criteria $criteria): array
    {
        if ($criteria->getAggregateRootTypes()) {
            throw new CriteriaNotSupportedException('DBAL implementation cannot support criteria based on aggregate root types.');
        }

        $bindValues = [];
        $bindValueTypes = [];

        $criteriaTypes = [];

        if ($criteria->getAggregateRootIds()) {
            $criteriaTypes[] = 'uuid IN (:uuids)';

            if ($this->useBinary) {
                $bindValues['uuids'] = [];
                foreach ($criteria->getAggregateRootIds() as $id) {
                    $bindValues['uuids'][] = $this->convertIdentifierToStorageValue($id);
                }
                $bindValueTypes['uuids'] = Connection::PARAM_STR_ARRAY;
            } else {
                $bindValues['uuids'] = $criteria->getAggregateRootIds();
                $bindValueTypes['uuids'] = Connection::PARAM_STR_ARRAY;
            }
        }

        if ($criteria->getEventTypes()) {
            $criteriaTypes[] = 'type IN (:types)';
            $bindValues['types'] = $criteria->getEventTypes();
            $bindValueTypes['types'] = Connection::PARAM_STR_ARRAY;
        }

        if (!$criteriaTypes) {
            return ['', [], []];
        }

        $where = 'WHERE '.join(' AND ', $criteriaTypes);

        return [$where, $bindValues, $bindValueTypes];
    }
}
