"""Payments publisher — writes to payments_db, then publishes to Kafka."""
import sqlite3
from broker import broker


def publish_payment(payment_id: str, amount: int, payments_db_path: str):
    conn = sqlite3.connect(payments_db_path, isolation_level=None)
    conn.execute("CREATE TABLE IF NOT EXISTS payments (id TEXT PRIMARY KEY, amount INT)")
    # BUG #1: commit happens BEFORE publish. If the broker call raises (or the
    # process crashes between these two lines) the payment is recorded in
    # payments_db but never makes it to the ledger consumer.
    conn.execute("INSERT INTO payments (id, amount) VALUES (?, ?)", (payment_id, amount))
    conn.commit()
    broker.publish("payments", {"payment_id": payment_id, "amount": amount})
    conn.close()
