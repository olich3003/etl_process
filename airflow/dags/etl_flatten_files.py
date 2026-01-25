import os
import csv
import json
import xml.etree.ElementTree as ET
from datetime import datetime

from airflow.decorators import dag, task


RAW = "/opt/airflow/data/raw"
OUT = "/opt/airflow/data/processed"

PETS_IN = f"{RAW}/pets-data.json"
NUTR_IN = f"{RAW}/nutrition.xml"

PETS_OUT = f"{OUT}/pets_flat.csv"
NUTR_OUT = f"{OUT}/nutrition_flat.csv"


def write_csv(rows, path):
    cols = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


@dag(
    dag_id="etl_flatten_files",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def main():
    @task
    def pets():
        os.makedirs(OUT, exist_ok=True)

        with open(PETS_IN, "r", encoding="utf-8") as f:
            data = json.load(f)

        rows = []
        for p in data.get("pets", []):
            base = {
                "name": p.get("name", ""),
                "species": p.get("species", ""),
                "birthYear": p.get("birthYear", ""),
                "photo": p.get("photo", ""),
            }
            foods = p.get("favFoods") or []
            if foods:
                for x in foods:
                    r = dict(base)
                    r["favFood"] = x
                    rows.append(r)
            else:
                r = dict(base)
                r["favFood"] = ""
                rows.append(r)

        write_csv(rows, PETS_OUT)
        return PETS_OUT

    @task
    def nutrition():
        os.makedirs(OUT, exist_ok=True)

        root = ET.parse(NUTR_IN).getroot()
        rows = []

        for food in root.findall(".//food"):
            r = {}
            for ch in list(food):
                t = ch.tag
                if t == "calories":
                    r["calories_total"] = ch.attrib.get("total", "")
                    r["calories_fat"] = ch.attrib.get("fat", "")
                elif t == "vitamins":
                    for v in list(ch):
                        r[f"vitamins_{v.tag}"] = (v.text or "").strip()
                elif t == "minerals":
                    for m in list(ch):
                        r[f"minerals_{m.tag}"] = (m.text or "").strip()
                else:
                    if len(list(ch)) == 0:
                        r[t] = (ch.text or "").strip()
                    else:
                        for sub in list(ch):
                            r[f"{t}_{sub.tag}"] = (sub.text or "").strip()

            rows.append(r)

        write_csv(rows, NUTR_OUT)
        return NUTR_OUT

    @task
    def done(p1, p2):
        return f"ok: {p1}, {p2}"

    done(pets(), nutrition())


main()

