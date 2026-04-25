"""Chaos test — proves the dual-write + non-idempotent consumer combo."""
import os
import sqlite3
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from broker import broker, BrokerPartition
from publisher import publish_payment
from consumer import LedgerConsumer


def test_zero_lost_writes():
    with tempfile.TemporaryDirectory() as tmp:
        payments_db = os.path.join(tmp, "payments.db")
        ledger_db = os.path.join(tmp, "ledger.db")
        consumer = LedgerConsumer(ledger_db)

        # Inject 4 partition events of 1 message each.
        for _ in range(4):
            broker.inject_partition(1)

        def make_payment(i):
            try:
                publish_payment(f"pmt_{i}", amount=100, payments_db_path=payments_db)
            except BrokerPartition:
                pass

        with ThreadPoolExecutor(max_workers=32) as ex:
            list(ex.map(make_payment, range(5000)))

        for offset, _, msg in broker.drain():
            consumer.handle(offset, msg)

        n_payments = sqlite3.connect(payments_db).execute("SELECT COUNT(*) FROM payments").fetchone()[0]
        n_ledger = sqlite3.connect(ledger_db).execute("SELECT COUNT(*) FROM ledger").fetchone()[0]

        assert n_payments == n_ledger, f"lost rows: {n_payments} payments vs {n_ledger} ledger entries"
