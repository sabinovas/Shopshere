"""Synthetic clickstream generator for ShopSphere (Week 3).

Simulates shopping sessions: each session views a few products, some views lead to
add_to_cart, and some carts end in a purchase. Events are JSON and are sent to the
Event Hub `clickstream`, or printed / written to a JSONL file with --dry-run.

Customer and product IDs are sampled from the Olist CSVs when --olist-dir is given,
so clicks join to the bronze tables. Otherwise random IDs are used.

Usage:
    # Preview 20 events, no Azure needed
    python clickstream_generator.py --dry-run --sessions 5

    # Send to Event Hubs (connection string from the environment, never a file)
    $env:EVENTHUB_CONN_STR = "<namespace connection string>"
    python clickstream_generator.py --olist-dir C:\\Users\\Sabino\\Downloads\\archive --rate 20 --minutes 10

    # Replay 3 hours of history in time order (closes session windows in one run)
    python clickstream_generator.py --sessions 3000 --backfill-hours 3

    # Late events (for the watermark demo): about 5% of events get a timestamp 2-15 minutes old
    python clickstream_generator.py --dry-run --sessions 50 --late-ratio 0.05
"""

import argparse
import csv
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

EVENTHUB_NAME = "clickstream"
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.6, 0.32, 0.08]
REFERRERS = ["google", "direct", "instagram", "facebook", "email", "affiliate"]

# Funnel probabilities, chosen so the funnel metric looks realistic
P_ADD_TO_CART = 0.30  # per product view
P_PURCHASE = 0.40  # per session that has a cart

fake = Faker("pt_BR")


def load_ids(olist_dir: Path | None, limit: int = 5000) -> tuple[list[str], list[tuple[str, str]]]:
    """Return (customer_ids, [(product_id, category)]) from the Olist CSVs, or random ones."""
    if olist_dir is None:
        customers = [uuid.uuid4().hex for _ in range(500)]
        products = [(uuid.uuid4().hex, random.choice(["bed_bath_table", "health_beauty", "sports_leisure"])) for _ in range(300)]
        return customers, products

    with open(olist_dir / "olist_customers_dataset.csv", newline="", encoding="utf-8") as f:
        customers = [row["customer_id"] for _, row in zip(range(limit), csv.DictReader(f))]
    with open(olist_dir / "olist_products_dataset.csv", newline="", encoding="utf-8") as f:
        products = [
            (row["product_id"], row["product_category_name"] or "unknown")
            for _, row in zip(range(limit), csv.DictReader(f))
        ]
    return customers, products


def make_session(customers, products, start: datetime) -> list[dict]:
    """Build one session's events in time order, starting at `start`."""
    session_id = uuid.uuid4().hex
    customer_id = random.choice(customers) if random.random() < 0.8 else None  # 20% anonymous
    device = random.choices(DEVICES, DEVICE_WEIGHTS)[0]
    referrer = random.choice(REFERRERS)
    ip = fake.ipv4_public()
    ts = start
    events, cart = [], []

    def event(event_type: str, product=None, price=None) -> dict:
        nonlocal ts
        ts += timedelta(seconds=random.randint(3, 90))
        return {
            "event_id": uuid.uuid4().hex,
            "event_ts": ts,
            "event_type": event_type,
            "session_id": session_id,
            "customer_id": customer_id,
            "product_id": product[0] if product else None,
            "category": product[1] if product else None,
            "price": price,
            "device": device,
            "referrer": referrer,
            "ip": ip,
            "user_agent": fake.user_agent(),
        }

    for product in random.sample(products, k=random.randint(1, 6)):
        price = round(random.uniform(9.9, 899.0), 2)
        events.append(event("view", product, price))
        if random.random() < P_ADD_TO_CART:
            events.append(event("add_to_cart", product, price))
            cart.append((product, price))

    if cart and random.random() < P_PURCHASE:
        for product, price in cart:
            events.append(event("purchase", product, price))
    return events


def finalize(e: dict, late_ratio: float) -> dict:
    """Serialise the timestamp; a share of events get a timestamp 2-15 minutes too old."""
    ts = e["event_ts"]
    if random.random() < late_ratio:
        ts -= timedelta(minutes=random.randint(2, 15))
    return {**e, "event_ts": ts.isoformat(timespec="milliseconds")}


def generate(args, customers, products):
    """Yield events in time order.

    Backfill mode (--backfill-hours): sessions start at random times over the last N hours,
    and all events are sorted by time before sending, like a replay of history.
    Live mode: sessions run one after another in real time, stamped with the send time.
    """
    if args.backfill_hours:
        now = datetime.now(timezone.utc)
        span = args.backfill_hours * 3600
        events = []
        for _ in range(args.sessions):
            start = now - timedelta(seconds=random.uniform(0, span))
            events.extend(make_session(customers, products, start))
        events.sort(key=lambda e: e["event_ts"])
        for e in events:
            if e["event_ts"] > now:  # sessions that started just before now would end in the future
                break
            yield finalize(e, args.late_ratio)
        return

    deadline = time.time() + args.minutes * 60 if args.minutes else None
    sessions_done = 0
    while True:
        if args.sessions and sessions_done >= args.sessions:
            return
        if deadline and time.time() >= deadline:
            return
        for e in make_session(customers, products, datetime.now(timezone.utc)):
            yield finalize({**e, "event_ts": datetime.now(timezone.utc)}, args.late_ratio)
            if args.rate:
                time.sleep(1 / args.rate)
        sessions_done += 1


def send_to_eventhub(events, batch_size: int):
    from azure.eventhub import EventData, EventHubProducerClient

    conn_str = os.environ.get("EVENTHUB_CONN_STR")
    if not conn_str:
        sys.exit("Set EVENTHUB_CONN_STR (namespace connection string) or use --dry-run.")

    producer = EventHubProducerClient.from_connection_string(conn_str, eventhub_name=EVENTHUB_NAME)
    sent = 0
    with producer:
        batch = producer.create_batch()
        for e in events:
            data = EventData(json.dumps(e))
            try:
                batch.add(data)
            except ValueError:  # batch full
                producer.send_batch(batch)
                batch = producer.create_batch()
                batch.add(data)
            if len(batch) >= batch_size:
                producer.send_batch(batch)
                sent += len(batch)
                print(f"sent {sent} events", flush=True)
                batch = producer.create_batch()
        if len(batch):
            producer.send_batch(batch)
            sent += len(batch)
    print(f"done: {sent} events sent to {EVENTHUB_NAME}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--olist-dir", type=Path, help="Folder with the Olist CSVs, to reuse real customer and product IDs")
    p.add_argument("--sessions", type=int, default=0, help="Stop after N sessions (0 = no limit)")
    p.add_argument("--minutes", type=float, default=0, help="Stop after N minutes (0 = no limit)")
    p.add_argument("--rate", type=float, default=0, help="Events per second (0 = as fast as possible)")
    p.add_argument("--backfill-hours", type=float, default=0, help="Spread --sessions over the last N hours and send in time order")
    p.add_argument("--late-ratio", type=float, default=0.0, help="Share of events with an old timestamp")
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--dry-run", action="store_true", help="Print events instead of sending them")
    p.add_argument("--out", type=Path, help="With --dry-run, write JSONL to this file instead of stdout")
    p.add_argument("--seed", type=int, help="Random seed, for repeatable output")
    args = p.parse_args()

    if not (args.sessions or args.minutes):
        p.error("give --sessions or --minutes so the generator stops")
    if args.backfill_hours and not args.sessions:
        p.error("--backfill-hours needs --sessions")
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    customers, products = load_ids(args.olist_dir)
    events = generate(args, customers, products)

    if args.dry_run:
        out = open(args.out, "w", encoding="utf-8") if args.out else sys.stdout
        n = 0
        for e in events:
            out.write(json.dumps(e) + "\n")
            n += 1
        if args.out:
            out.close()
            print(f"wrote {n} events to {args.out}")
    else:
        send_to_eventhub(events, args.batch_size)


if __name__ == "__main__":
    main()
