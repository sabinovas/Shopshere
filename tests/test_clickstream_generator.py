import argparse
import random
from datetime import datetime, timedelta, timezone

import pytest
from conftest import load_module

gen = load_module("data_generator/clickstream_generator.py", "clickstream_generator")

CUSTOMERS = [f"c{i}" for i in range(50)]
PRODUCTS = [(f"p{i}", "cat") for i in range(40)]


@pytest.fixture(autouse=True)
def seed():
    random.seed(7)
    gen.Faker.seed(7)


def test_session_events_are_in_time_order_and_share_a_session():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events = gen.make_session(CUSTOMERS, PRODUCTS, start)
    assert events, "a session has at least one view"
    assert len({e["session_id"] for e in events}) == 1
    times = [e["event_ts"] for e in events]
    assert times == sorted(times)
    assert all(t > start for t in times)


def test_funnel_rules_hold_across_many_sessions():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for _ in range(300):
        events = gen.make_session(CUSTOMERS, PRODUCTS, start)
        viewed = {e["product_id"] for e in events if e["event_type"] == "view"}
        carted = {e["product_id"] for e in events if e["event_type"] == "add_to_cart"}
        bought = {e["product_id"] for e in events if e["event_type"] == "purchase"}
        assert carted <= viewed, "only viewed products are added to the cart"
        assert bought <= carted, "only carted products are bought"
        assert not bought or bought == carted, "a purchase buys the whole cart"


def test_finalize_shifts_late_events_into_the_past():
    ts = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    on_time = gen.finalize({"event_ts": ts}, late_ratio=0.0)
    late = gen.finalize({"event_ts": ts}, late_ratio=1.0)
    assert datetime.fromisoformat(on_time["event_ts"]) == ts
    shift = ts - datetime.fromisoformat(late["event_ts"])
    assert timedelta(minutes=2) <= shift <= timedelta(minutes=15)


def test_backfill_is_sorted_and_never_in_the_future():
    args = argparse.Namespace(backfill_hours=2, sessions=200, late_ratio=0.0, minutes=0, rate=0)
    events = list(gen.generate(args, CUSTOMERS, PRODUCTS))
    times = [datetime.fromisoformat(e["event_ts"]) for e in events]
    assert times == sorted(times)
    assert max(times) <= datetime.now(timezone.utc)
    assert min(times) >= datetime.now(timezone.utc) - timedelta(hours=2, minutes=1)
