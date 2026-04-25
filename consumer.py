"""Ledger consumer — reads from broker, writes to ledger_db."""
import sqlite3


class LedgerConsumer:
    def __init__(self, ledger_db_path: str):
        self.conn = sqlite3.connect(ledger_db_path, isolation_level=None)
        self.conn.execute("CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY, payment_id TEXT, amount INT)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS processed_offsets (offset INTEGER PRIMARY KEY)")

    def handle(self, offset: int, message: dict):
        # BUG #2: dedup key is the Kafka OFFSET. On rebalance the offset can be
        # different for the same logical message, so this dedup is useless.
        existing = self.conn.execute(
            "SELECT 1 FROM processed_offsets WHERE offset = ?", (offset,)
        ).fetchone()
        if existing:
            return
        self.conn.execute("INSERT INTO processed_offsets (offset) VALUES (?)", (offset,))
        self.conn.execute(
            "INSERT INTO ledger (payment_id, amount) VALUES (?, ?)",
            (message["payment_id"], message["amount"]),
        )
