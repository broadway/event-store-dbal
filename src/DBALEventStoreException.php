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

use Broadway\EventStore\EventStoreException;
use Doctrine\DBAL\Exception;

/**
 * Wraps exceptions thrown by the DBAL event store.
 */
class DBALEventStoreException extends EventStoreException
{
    public static function create(Exception $exception): DBALEventStoreException
    {
        return new DBALEventStoreException('', 0, $exception);
    }
}
