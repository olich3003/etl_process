#!/usr/bin/env python3
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


REGIONS = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "DE-SN", "DE-BW"]
CAMPAIGNS = ["credit_card_offer", "cash_loan", "mortgage", "refinance", "car_loan"]
CALL_STATUSES = ["answered", "missed", "busy", "failed"]
RESPONSES = ["interested", "not_interested", "callback", "no_answer", "declined"]


def build_row(i: int, base_time: datetime) -> list[str]:
    call_time = base_time + timedelta(seconds=random.randint(0, 31 * 24 * 3600))
    status = random.choices(CALL_STATUSES, weights=[72, 13, 8, 7], k=1)[0]
    response = random.choice(RESPONSES if status == "answered" else ["no_answer"])
    duration = random.randint(12, 420) if status == "answered" else random.randint(0, 20)
    return [
        f"call_202605_{i:09d}",
        call_time.strftime("%Y-%m-%d %H:%M:%S"),
        f"client_{random.randint(1000, 999999)}",
        random.choice(REGIONS),
        random.choice(CAMPAIGNS),
        status,
        response,
        str(duration),
        str(response in {"interested", "callback"}).lower(),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate transactions_v2 CSV with target size.")
    parser.add_argument("--output", default="data/generated/transactions_v2.csv")
    parser.add_argument("--min-mb", type=float, default=32.0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    min_bytes = int(args.min_mb * 1024 * 1024)

    headers = [
        "call_id",
        "call_time",
        "client_id",
        "region_code",
        "campaign_type",
        "call_status",
        "client_response",
        "duration_sec",
        "follow_up_required",
    ]

    base_time = datetime(2026, 5, 1, 0, 0, 0)
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

