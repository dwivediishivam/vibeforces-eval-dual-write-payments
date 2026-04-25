"""Tiny in-process Kafka-shaped broker. Supports partition injection."""
import threading
import random


class BrokerPartition(Exception):
    pass


class Broker:
    def __init__(self):
        self._messages = []
        self._lock = threading.Lock()
        self._partition_until = 0
        self._counter = 0

    def publish(self, topic, msg):
        with self._lock:
            if self._counter < self._partition_until:
                self._counter += 1
                raise BrokerPartition("simulated network partition")
            self._counter += 1
            self._messages.append((len(self._messages), topic, msg))

    def inject_partition(self, count_messages: int):
        with self._lock:
            self._partition_until = self._counter + count_messages

    def drain(self):
        with self._lock:
            out = list(self._messages)
            self._messages.clear()
            return out


broker = Broker()
