#!/usr/bin/env python3
import argparse
import json
import time
from pathlib import Path

import jwt
import requests


IAM_TOKEN_URL = "https://iam.api.cloud.yandex.net/iam/v1/tokens"


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Yandex Cloud IAM token from service account authorized key.")
    parser.add_argument("--sa-key-file", required=True)
    parser.add_argument("--output", default="iam-token.txt")
    args = parser.parse_args()

    key = json.loads(Path(args.sa_key_file).read_text(encoding="utf-8"))
    now = int(time.time())
    payload = {
        "aud": IAM_TOKEN_URL,
        "iss": key["service_account_id"],
        "iat": now,
        "exp": now + 3600,
    }
    headers = {"kid": key["id"]}
    encoded_jwt = jwt.encode(payload, key["private_key"], algorithm="PS256", headers=headers)

    response = requests.post(IAM_TOKEN_URL, json={"jwt": encoded_jwt}, timeout=30)
    response.raise_for_status()
    iam_token = response.json()["iamToken"]
    Path(args.output).write_text(iam_token, encoding="utf-8")
    print(f"IAM token saved to {args.output}. Expires in about 12 hours according to IAM response.")


if __name__ == "__main__":
    main()

