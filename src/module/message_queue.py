"""Pluggable process-safe message bus.

The historical class name ``RabbitMQ`` remains as a compatibility facade.
With ``rabbitmq.backend: sqlite`` no broker process is required.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Literal

from .base import Base
from .utils import load_config


class _SQLiteChannel:
    def __init__(self, owner: 'RabbitMQ') -> None:
        self.owner = owner

    def basic_ack(self, delivery_tag: int) -> None:
        self.owner._ack(delivery_tag)


class RabbitMQ(Base):
    """Compatibility API backed by SQLite by default or RabbitMQ optionally."""

    def __init__(
        self,
        exchange_name: str,
        exchange_type: Literal['direct', 'topic', 'headers', 'fanout'] = 'direct',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        config = load_config().get('rabbitmq', {})
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.backend = os.environ.get(
            'ACV_MESSAGE_BUS_BACKEND', config.get('backend', 'sqlite')
        ).lower()
        self.connection = None
        self.channel = None
        self._consumer_id = str(uuid.uuid4())

        if self.backend == 'sqlite':
            configured_path = os.environ.get(
                'ACV_MESSAGE_DB', config.get('sqlite_path', 'message_bus.sqlite3')
            )
            self.db_path = str(Path(configured_path).resolve())
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._initialize_sqlite()
            self.channel = _SQLiteChannel(self)
        elif self.backend == 'rabbitmq':
            self._initialize_rabbitmq(config)
        else:
            raise ValueError(
                f'Unsupported message bus backend {self.backend!r}; '
                'expected "sqlite" or "rabbitmq".'
            )

    def _connect_sqlite(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.execute('PRAGMA busy_timeout=30000')
        return connection

    def _initialize_sqlite(self) -> None:
        with self._connect_sqlite() as connection:
            connection.execute('PRAGMA journal_mode=WAL')
            connection.executescript(
                '''
                CREATE TABLE IF NOT EXISTS queue_bindings (
                    exchange_name TEXT NOT NULL,
                    queue_name TEXT NOT NULL,
                    routing_key TEXT NOT NULL,
                    PRIMARY KEY (exchange_name, queue_name, routing_key)
                );
                CREATE TABLE IF NOT EXISTS queue_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue_name TEXT NOT NULL,
                    body BLOB NOT NULL,
                    headers_json TEXT NOT NULL,
                    claimed_by TEXT,
                    claimed_at REAL
                );
                CREATE INDEX IF NOT EXISTS idx_queue_messages_ready
                    ON queue_messages(queue_name, claimed_by, id);
                '''
            )

    def _initialize_rabbitmq(self, config: dict) -> None:
        try:
            import pika
        except ImportError as exc:
            raise RuntimeError(
                'The pika package is required only when rabbitmq.backend is "rabbitmq".'
            ) from exc

        url = os.environ.get('RABBITMQ_URL', config.get('url', 'amqp://guest:guest@localhost/'))
        # Preserve support for the old Celery-style pyamqp URL.
        if url.startswith('pyamqp://'):
            url = 'amqp://' + url[len('pyamqp://'):]
        parameters = pika.URLParameters(url)
        parameters.heartbeat = 180
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=self.exchange_type,
        )

    def add_queue(
        self,
        name: str,
        routing_keys: list[str],
        exclusive: bool = False,
        auto_delete: bool = False,
    ) -> None:
        if self.backend == 'rabbitmq':
            self.channel.queue_declare(
                queue=name,
                durable=True,
                exclusive=exclusive,
                auto_delete=auto_delete,
            )
            for routing_key in routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=name,
                    routing_key=routing_key,
                )
            return

        with self._connect_sqlite() as connection:
            connection.executemany(
                '''
                INSERT OR IGNORE INTO queue_bindings
                    (exchange_name, queue_name, routing_key)
                VALUES (?, ?, ?)
                ''',
                [
                    (self.exchange_name, name, routing_key)
                    for routing_key in routing_keys
                ],
            )

    def publish(
        self,
        message: str,
        routing_keys: list[str],
        headers: dict | None = None,
    ) -> None:
        headers = headers or {}
        if self.backend == 'rabbitmq':
            import pika

            properties = pika.BasicProperties(delivery_mode=2, headers=headers)
            for routing_key in routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=properties,
                )
            return

        body = message.encode('utf-8') if isinstance(message, str) else message
        headers_json = json.dumps(headers)
        with self._connect_sqlite() as connection:
            for routing_key in routing_keys:
                queues = connection.execute(
                    '''
                    SELECT queue_name FROM queue_bindings
                    WHERE exchange_name = ? AND routing_key = ?
                    ''',
                    (self.exchange_name, routing_key),
                ).fetchall()
                connection.executemany(
                    '''
                    INSERT INTO queue_messages (queue_name, body, headers_json)
                    VALUES (?, ?, ?)
                    ''',
                    [
                        (queue_name, body, headers_json)
                        for (queue_name,) in queues
                    ],
                )

    def _claim(self, queue: str):
        with self._connect_sqlite() as connection:
            connection.execute('BEGIN IMMEDIATE')
            row = connection.execute(
                '''
                SELECT id, body, headers_json FROM queue_messages
                WHERE queue_name = ? AND claimed_by IS NULL
                ORDER BY id LIMIT 1
                ''',
                (queue,),
            ).fetchone()
            if row is None:
                return None
            updated = connection.execute(
                '''
                UPDATE queue_messages
                SET claimed_by = ?, claimed_at = ?
                WHERE id = ? AND claimed_by IS NULL
                ''',
                (self._consumer_id, time.time(), row[0]),
            )
            if updated.rowcount != 1:
                return None
            return row

    def _ack(self, delivery_tag: int) -> None:
        with self._connect_sqlite() as connection:
            connection.execute(
                'DELETE FROM queue_messages WHERE id = ? AND claimed_by = ?',
                (delivery_tag, self._consumer_id),
            )

    def subscribe(
        self,
        queue: str,
        callback: Callable,
        auto_ack: bool = False,
        stop_event=None,
    ) -> None:
        if self.backend == 'rabbitmq':
            if stop_event is not None:
                self.info(f'Subscribed to queue: {queue}')
                for method, properties, body in self.channel.consume(
                    queue=queue,
                    auto_ack=auto_ack,
                    inactivity_timeout=0.5,
                ):
                    if stop_event.is_set():
                        break
                    if method is not None:
                        callback(self.channel, method, properties, body)
                self.channel.cancel()
                return
            self.channel.basic_consume(
                queue=queue,
                on_message_callback=callback,
                auto_ack=auto_ack,
            )
            self.info(f'Subscribed to queue: {queue}')
            self.channel.start_consuming()
            return

        self.info(f'Subscribed to SQLite queue: {queue}')
        channel = _SQLiteChannel(self)
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            row = self._claim(queue)
            if row is None:
                time.sleep(0.2)
                continue
            message_id, body, headers_json = row
            method = SimpleNamespace(delivery_tag=message_id)
            properties = SimpleNamespace(headers=json.loads(headers_json))
            try:
                callback(channel, method, properties, body)
                if auto_ack:
                    self._ack(message_id)
            except BaseException:
                with self._connect_sqlite() as connection:
                    connection.execute(
                        '''
                        UPDATE queue_messages
                        SET claimed_by = NULL, claimed_at = NULL
                        WHERE id = ? AND claimed_by = ?
                        ''',
                        (message_id, self._consumer_id),
                    )
                raise

    def close(self) -> None:
        if self.backend == 'rabbitmq' and self.connection:
            self.connection.close()
