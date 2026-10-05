"""
Generate realistic, deliberately messy raw data that mimics Dubai Land Department
(DLD) transaction exports, so the pipeline runs end to end with no downloads.

Swap this out for the real DLD open-data CSV by pointing etl_pipeline.py at it
(column mapping lives in etl_pipeline.COLUMN_MAP).

Outputs (data/raw/):
  area_master_v1.csv        area reference data, original zoning
  area_master_v2.csv        same areas, some re-zoned (drives SCD Type 2)
  transactions_batch1.csv   2020-01 .. 2025-12  (initial / historical load)
  transactions_batch2.csv   2026-01 .. 2026-09  (incremental load)
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

RNG = np.random.default_rng(42)

AREAS = [
    # area_id, area_name, community, zone, base AED/sqm
    (1, "Downtown Dubai", "Downtown", "Central", 24000),
    (2, "Dubai Marina", "Marina", "Waterfront", 19500),
    (3, "Palm Jumeirah", "Palm", "Waterfront", 32000),
    (4, "Business Bay", "Business Bay", "Central", 18500),
    (5, "Jumeirah Village Circle", "JVC", "Suburban", 11500),
    (6, "Dubai Hills Estate", "Dubai Hills", "Suburban", 17000),
    (7, "Dubai South", "Dubai South", "Emerging", 8500),
    (8, "Arjan", "Arjan", "Suburban", 10500),
    (9, "Al Barsha South", "Al Barsha", "Suburban", 12500),
    (10, "Dubai Creek Harbour", "Creek", "Central", 20000),
    (11, "Meydan", "Meydan", "Emerging", 14500),
    (12, "Jumeirah Lakes Towers", "JLT", "Waterfront", 15500),
]
# Areas re-zoned in the v2 master (effective 2026-01-01) -> SCD2 history
REZONED = {5: "Emerging", 7: "Suburban", 11: "Central"}
REZONE_DATE = "2026-01-01"

PROPERTY_TYPES = [("Unit", 0.72), ("Villa", 0.18), ("Land", 0.10)]
ROOMS = ["Studio", "1 B/R", "2 B/R", "3 B/R", "4 B/R", "5+ B/R"]
TXN_TYPES = [("Sales", 0.78), ("Mortgage", 0.17), ("Gifts", 0.05)]
BUYER_TYPES = [("Individual", 0.62), ("Investor", 0.25), ("Corporate", 0.13)]


def pick(options, n):
    labels, probs = zip(*options)
    return RNG.choice(labels, size=n, p=probs)


def make_transactions(start, end, n, id_offset):
    dates = pd.to_datetime(RNG.choice(pd.date_range(start, end, freq="D"), size=n))
    area_idx = RNG.choice(len(AREAS), size=n, p=np.array(
        [14, 13, 6, 12, 14, 8, 6, 7, 7, 5, 4, 4]) / 100)
    areas = [AREAS[i] for i in area_idx]

    ptype = pick(PROPERTY_TYPES, n)
    rooms = np.where(ptype == "Land", "", RNG.choice(ROOMS, size=n, p=[.08, .27, .30, .18, .11, .06]))
    size = np.where(
        ptype == "Land", RNG.normal(900, 300, n),
        np.where(ptype == "Villa", RNG.normal(380, 120, n), RNG.normal(95, 40, n)),
    ).clip(25, None)

    years = (dates - pd.Timestamp("2020-01-01")).days / 365.25
    market = 1 + 0.11 * years + 0.04 * np.sin(years * 2 * np.pi)  # trend + seasonality
    base = np.array([a[4] for a in areas])
    sqm_price = base * market * RNG.normal(1, 0.12, n) * np.where(ptype == "Villa", 0.85, 1)
    value = (sqm_price * size).round(0)

    df = pd.DataFrame({
        "TRANSACTION_ID": [f"TXN{id_offset + i:08d}" for i in range(n)],
        "INSTANCE_DATE": dates.strftime("%d-%m-%Y"),
        "AREA_ID": [a[0] for a in areas],
        "AREA_NAME_EN": [a[1] for a in areas],
        "PROP_TYPE_EN": ptype,
        "ROOMS_EN": rooms,
        "OFFPLAN": RNG.choice(["Off-Plan", "Ready"], size=n, p=[.45, .55]),
        "TRANS_GROUP_EN": pick(TXN_TYPES, n),
        "BUYER_TYPE": pick(BUYER_TYPES, n),
        "PROCEDURE_AREA": size.round(1),
        "ACTUAL_WORTH": value,
    })

    # ---- inject realistic dirt ----
    k = max(n // 100, 5)
    df.loc[RNG.choice(n, k), "ACTUAL_WORTH"] = np.nan                    # missing values
    df.loc[RNG.choice(n, k), "PROCEDURE_AREA"] = 0                       # bad area
    df.loc[RNG.choice(n, k), "ACTUAL_WORTH"] = -1 * df["ACTUAL_WORTH"].abs()  # negatives
    df.loc[RNG.choice(n, k), "PROP_TYPE_EN"] = df["PROP_TYPE_EN"].str.upper()  # casing
    df.loc[RNG.choice(n, k), "AREA_NAME_EN"] = "  " + df["AREA_NAME_EN"] + " "  # whitespace
    df = pd.concat([df, df.sample(k, random_state=1)], ignore_index=True)        # duplicates
    return df.sample(frac=1, random_state=7).reset_index(drop=True)


def main(out_dir, rows1, rows2):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    v1 = pd.DataFrame(AREAS, columns=["AREA_ID", "AREA_NAME_EN", "COMMUNITY", "ZONE", "_base"]).drop(columns="_base")
    v2 = v1.copy()
    v2["ZONE"] = v2.apply(lambda r: REZONED.get(r.AREA_ID, r.ZONE), axis=1)
    v1["EFFECTIVE_DATE"] = "2020-01-01"
    v2["EFFECTIVE_DATE"] = REZONE_DATE
    v1.to_csv(out / "area_master_v1.csv", index=False)
    v2.to_csv(out / "area_master_v2.csv", index=False)

    make_transactions("2020-01-01", "2025-12-31", rows1, 1).to_csv(out / "transactions_batch1.csv", index=False)
    make_transactions("2026-01-01", "2026-09-30", rows2, 5_000_001).to_csv(out / "transactions_batch2.csv", index=False)
    print(f"Wrote raw files to {out.resolve()}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--rows1", type=int, default=250_000)
    ap.add_argument("--rows2", type=int, default=30_000)
    a = ap.parse_args()
    main(a.out, a.rows1, a.rows2)
