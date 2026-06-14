#!/usr/bin/env python3
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


REGIONS = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "DE-SN", "DE-BW"]
PRODUCTS = ["cash_loan", "credit_card", "mortgage", "car_loan", "refinance"]
CHANNELS = ["mobile", "web", "branch", "call_center", "partner"]


def risk_from_score(score: int) -> str:
    if score >= 720:
        return "low"
    if score >= 620:
        return "medium"
    return "high"


def decision_from_risk(risk: str) -> str:
    if risk == "low":
        return random.choices(["approved", "manual_review"], weights=[86, 14], k=1)[0]
    if risk == "medium":
        return random.choices(["approved", "manual_review", "rejected"], weights=[45, 35, 20], k=1)[0]
    return random.choices(["manual_review", "rejected"], weights=[28, 72], k=1)[0]


def build_row(i: int, base_time: datetime) -> list[str]:
    event_time = base_time + timedelta(seconds=random.randint(0, 31 * 24 * 3600))
    amount = random.randrange(2000, 80000, 500)
    term = random.choice([6, 12, 18, 24, 36, 48, 60])
    score = random.randint(480, 850)
    risk = risk_from_score(score)
    decision = decision_from_risk(risk)
    approved_amount = amount if decision == "approved" else 0
    if decision == "manual_review":
        approved_amount = random.choice([0, amount, int(amount * 0.8)])

    return [
        f"app_202605_{i:09d}",
        event_time.strftime("%Y-%m-%d %H:%M:%S"),
        f"cust_{random.randint(10000, 999999)}",
        random.choice(REGIONS),
        random.choice(PRODUCTS),
        str(amount),
        str(term),
        str(score),
        risk,
        decision,
        str(approved_amount),
        random.choice(CHANNELS),
        str(decision == "manual_review").lower(),
        str(random.randint(3, 240)),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate loan applications CSV with target size.")
    parser.add_argument("--output", default="data/generated/applications.csv")
    parser.add_argument("--min-mb", type=float, default=55.0)
    parser.add_argument("--seed", type=int, default=43)
    args = parser.parse_args()

    random.seed(args.seed)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    min_bytes = int(args.min_mb * 1024 * 1024)
    base_time = datetime(2026, 5, 1, 0, 0, 0)

    headers = [
        "application_id",
        "event_time",
        "customer_id",
        "region_code",
        "product_type",
        "requested_amount",
        "term_months",
        "credit_score",
        "risk_level",
        "decision_status",
        "approved_amount",
        "channel",
        "employee_review_flag",
        "processing_time_sec",
    ]

    rows = 0
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        while f.tell() < min_bytes:
            rows += 1
            writer.writerow(build_row(rows, base_time))

    size_mb = output.stat().st_size / 1024 / 1024
    print(f"Generated {rows} rows at {output} ({size_mb:.2f} MB)")


if __name__ == "__main__":
    main()

