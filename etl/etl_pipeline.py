"""
End-to-end ETL for the Dubai Real Estate Data Warehouse.

  extract -> stage -> clean/validate -> dimensions (SCD2 on dim_area)
          -> incremental fact load -> data quality checks -> views -> CSV export

Usage:
  python etl/etl_pipeline.py --transactions data/raw/transactions_batch1.csv \
                             --area-master  data/raw/area_master_v1.csv
  python etl/etl_pipeline.py --transactions data/raw/transactions_batch2.csv \
                             --area-master  data/raw/area_master_v2.csv

Target DB is any SQLAlchemy URL (default: local SQLite file). For Power BI use
MySQL / PostgreSQL / SQL Server, e.g.  --db "mysql+pymysql://user:pw@host/dubai_dw"
"""
import argparse
import hashlib
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
OPEN_END = pd.Timestamp("9999-12-31")

# raw (DLD-style) column -> internal name. Change this map for real DLD files.
COLUMN_MAP = {
    "TRANSACTION_ID": "transaction_id", "INSTANCE_DATE": "instance_date",
    "AREA_ID": "area_id", "AREA_NAME_EN": "area_name_en",
    "PROP_TYPE_EN": "prop_type_en", "ROOMS_EN": "rooms_en", "OFFPLAN": "offplan",
    "TRANS_GROUP_EN": "trans_group_en", "BUYER_TYPE": "buyer_type",
    "PROCEDURE_AREA": "procedure_area", "ACTUAL_WORTH": "actual_worth",
}

log = logging.getLogger("etl")


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def run_sql_file(engine, path):
    lines = Path(path).read_text().splitlines()
    sql = "\n".join(l for l in lines if not l.strip().startswith("--"))  # drop comments
    with engine.begin() as con:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            con.execute(text(stmt))


def next_key(engine, table, col):
    with engine.connect() as con:
        v = con.execute(text(f"SELECT COALESCE(MAX({col}), 0) FROM {table}")).scalar()
    return int(v) + 1


# --------------------------------------------------------------------------
# extract + stage
# --------------------------------------------------------------------------
def extract(path):
    df = pd.read_csv(path, dtype={"TRANSACTION_ID": str})
    df = df.rename(columns=COLUMN_MAP)[list(COLUMN_MAP.values())]
    log.info("extracted %s rows from %s", f"{len(df):,}", Path(path).name)
    return df


def stage(engine, df, area_df, batch_id):
    s = df.copy()
    s["_batch_id"], s["_loaded_at"] = batch_id, now()
    with engine.begin() as con:
        con.execute(text("DELETE FROM stg_transactions"))   # staging is transient
        con.execute(text("DELETE FROM stg_area_master"))
    s.to_sql("stg_transactions", engine, if_exists="append", index=False, chunksize=20_000)
    a = area_df.rename(columns=str.lower).copy()
    a["_batch_id"] = batch_id
    a.to_sql("stg_area_master", engine, if_exists="append", index=False)


# --------------------------------------------------------------------------
# clean + validate
# --------------------------------------------------------------------------
def clean(df, batch_id):
    """Standardise, then split into (good rows, rejected rows, duplicate count)."""
    d = df.copy()
    d["transaction_id"] = d["transaction_id"].astype(str).str.strip()
    for c in ["area_name_en", "prop_type_en", "rooms_en", "offplan", "trans_group_en", "buyer_type"]:
        d[c] = d[c].fillna("").astype(str).str.strip()
    d["prop_type_en"] = d["prop_type_en"].str.title()
    d["rooms_en"] = d["rooms_en"].replace("", "N/A")
    d["txn_date"] = pd.to_datetime(d["instance_date"], format="%d-%m-%Y", errors="coerce")

    before = len(d)
    d = d.drop_duplicates(subset="transaction_id", keep="first")
    dupes = before - len(d)

    reasons = pd.Series("", index=d.index)
    reasons[d["txn_date"].isna()] = "invalid_date"
    reasons[d["actual_worth"].isna()] = "missing_value"
    reasons[(d["actual_worth"] <= 0) & d["actual_worth"].notna()] = "non_positive_value"
    reasons[d["procedure_area"].fillna(0) <= 0] = "invalid_area_sqm"
    reasons[d["area_id"].isna()] = "missing_area"

    rejects = d[reasons != ""][["transaction_id"]].assign(
        reject_reason=reasons[reasons != ""], _batch_id=batch_id, _rejected_at=now())
    good = d[reasons == ""].copy()
    good["price_per_sqm"] = (good["actual_worth"] / good["procedure_area"]).round(2)
    return good, rejects, dupes


# --------------------------------------------------------------------------
# dimensions
# --------------------------------------------------------------------------
def load_dim_date(engine, start="2020-01-01", end="2027-12-31"):
    with engine.connect() as con:
        if con.execute(text("SELECT COUNT(*) FROM dim_date")).scalar():
            return
    d = pd.DataFrame({"full_date": pd.date_range(start, end, freq="D")})
    f = d["full_date"]
    out = pd.DataFrame({
        "date_key": f.dt.strftime("%Y%m%d").astype(int),
        "full_date": f.dt.strftime("%Y-%m-%d"),
        "day_of_month": f.dt.day, "day_name": f.dt.day_name(),
        "day_of_week": f.dt.dayofweek + 1,
        "is_weekend": f.dt.dayofweek.isin([5, 6]).astype(int),
        "week_of_year": f.dt.isocalendar().week.astype(int),
        "month_num": f.dt.month, "month_name": f.dt.month_name(),
        "year_month": f.dt.strftime("%Y-%m"),
        "quarter_num": f.dt.quarter,
        "quarter_label": f.dt.year.astype(str) + "-Q" + f.dt.quarter.astype(str),
        "year_num": f.dt.year,
    })
    out.to_sql("dim_date", engine, if_exists="append", index=False, chunksize=5_000)
    log.info("dim_date populated: %s days", f"{len(out):,}")


def load_simple_dim(engine, table, key_col, value_cols, values_df):
    """Insert-only conformed dimension (Type 1 / no history)."""
    values_df = values_df[value_cols].drop_duplicates()
    existing = pd.read_sql(f"SELECT {', '.join(value_cols)} FROM {table}", engine)
    new = values_df.merge(existing, on=value_cols, how="left", indicator=True)
    new = new[new["_merge"] == "left_only"].drop(columns="_merge")
    if new.empty:
        return 0
    start = next_key(engine, table, key_col)
    new.insert(0, key_col, range(start, start + len(new)))
    new.to_sql(table, engine, if_exists="append", index=False)
    return len(new)


def row_hash(*vals):
    return hashlib.md5("|".join(map(str, vals)).encode()).hexdigest()


def load_dim_area_scd2(engine, area_df):
    """
    SCD Type 2. Compare incoming attributes (hash) against the current row.
      - new area            -> insert current row (valid_from = effective date)
      - attributes changed  -> close old row (valid_to = effective - 1 day,
                               is_current = 0) and insert a new current row
      - unchanged           -> no-op
    """
    inc = area_df.rename(columns=str.lower).copy()
    inc["effective_date"] = pd.to_datetime(inc["effective_date"])
    inc["row_hash"] = [row_hash(r.area_name_en, r.community, r.zone) for r in inc.itertuples()]

    cur = pd.read_sql("SELECT * FROM dim_area WHERE is_current = 1", engine)
    merged = inc.merge(cur[["area_key", "area_id", "row_hash"]], on="area_id",
                       how="left", suffixes=("", "_cur"))
    new_rows = merged[merged["area_key"].isna()]
    changed = merged[merged["area_key"].notna() & (merged["row_hash"] != merged["row_hash_cur"])]

    with engine.begin() as con:
        for r in changed.itertuples():
            con.execute(text(
                "UPDATE dim_area SET valid_to = :vt, is_current = 0 WHERE area_key = :k"),
                {"vt": (r.effective_date - pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                 "k": int(r.area_key)})

    to_insert = pd.concat([new_rows, changed])
    if not to_insert.empty:
        k0 = next_key(engine, "dim_area", "area_key")
        ins = pd.DataFrame({
            "area_key": range(k0, k0 + len(to_insert)),
            "area_id": to_insert["area_id"].values,
            "area_name": to_insert["area_name_en"].values,
            "community": to_insert["community"].values,
            "zone": to_insert["zone"].values,
            "valid_from": to_insert["effective_date"].dt.strftime("%Y-%m-%d").values,
            "valid_to": OPEN_END.strftime("%Y-%m-%d"),
            "is_current": 1,
            "row_hash": to_insert["row_hash"].values,
        })
        ins.to_sql("dim_area", engine, if_exists="append", index=False)
    return len(new_rows), len(changed)


# --------------------------------------------------------------------------
# fact (incremental)
# --------------------------------------------------------------------------
def load_fact(engine, good, batch_id):
    # incremental: skip anything already loaded (idempotent re-runs)
    have = pd.read_sql("SELECT transaction_id FROM fact_transactions", engine)
    new = good[~good["transaction_id"].isin(have["transaction_id"])].copy()
    if new.empty:
        return 0

    # --- surrogate key lookups ---
    # area: point-in-time join so each sale maps to the area version valid on its date
    area = pd.read_sql("SELECT area_key, area_id, valid_from, valid_to FROM dim_area", engine,
                       parse_dates=["valid_from", "valid_to"])
    new = new.merge(area, on="area_id", how="left")
    new = new[(new["txn_date"] >= new["valid_from"]) & (new["txn_date"] <= new["valid_to"])]

    prop = pd.read_sql("SELECT * FROM dim_property", engine).rename(
        columns={"property_type": "prop_type_en", "rooms": "rooms_en", "sale_stage": "offplan"})
    new = new.merge(prop, on=["prop_type_en", "rooms_en", "offplan"], how="left")
    new = new.merge(pd.read_sql("SELECT * FROM dim_buyer_type", engine), on="buyer_type", how="left")
    new = new.merge(pd.read_sql("SELECT * FROM dim_transaction_type", engine)
                    .rename(columns={"txn_type": "trans_group_en"}), on="trans_group_en", how="left")

    k0 = next_key(engine, "fact_transactions", "transaction_key")
    fact = pd.DataFrame({
        "transaction_key": range(k0, k0 + len(new)),
        "transaction_id": new["transaction_id"].values,
        "date_key": new["txn_date"].dt.strftime("%Y%m%d").astype(int).values,
        "area_key": new["area_key"].values,
        "property_key": new["property_key"].values,
        "buyer_type_key": new["buyer_type_key"].values,
        "txn_type_key": new["txn_type_key"].values,
        "transaction_value": new["actual_worth"].values,
        "area_sqm": new["procedure_area"].values,
        "price_per_sqm": new["price_per_sqm"].values,
        "_batch_id": batch_id,
    })
    fact.to_sql("fact_transactions", engine, if_exists="append", index=False, chunksize=20_000)
    return len(fact)


# --------------------------------------------------------------------------
# data quality
# --------------------------------------------------------------------------
def run_dq(engine, run_id, extracted, rejected, dupes, loaded):
    checks = []

    def add(name, ok, detail, warn=False):
        checks.append((run_id, name, "PASS" if ok else ("WARN" if warn else "FAIL"), detail, now()))

    q = lambda s: pd.read_sql(s, engine).iloc[0, 0]

    # 1. row reconciliation: extracted = loaded + rejected + duplicates (+ already loaded)
    already = extracted - dupes - rejected - loaded
    add("row_reconciliation", already >= 0,
        f"extracted={extracted:,} loaded={loaded:,} rejected={rejected:,} "
        f"duplicates={dupes:,} already_loaded={already:,}")
    # 2. referential integrity: every FK resolves
    for dim, key, fk in [("dim_date", "date_key", "date_key"), ("dim_area", "area_key", "area_key"),
                         ("dim_property", "property_key", "property_key"),
                         ("dim_buyer_type", "buyer_type_key", "buyer_type_key"),
                         ("dim_transaction_type", "txn_type_key", "txn_type_key")]:
        orphans = q(f"SELECT COUNT(*) FROM fact_transactions f LEFT JOIN {dim} d "
                    f"ON f.{fk} = d.{key} WHERE d.{key} IS NULL")
        add(f"fk_{dim}", orphans == 0, f"orphans={orphans}")
    # 3. null checks on measures
    nulls = q("SELECT COUNT(*) FROM fact_transactions WHERE transaction_value IS NULL "
              "OR area_sqm IS NULL OR price_per_sqm IS NULL")
    add("null_measures", nulls == 0, f"null_rows={nulls}")
    # 4. uniqueness of business key
    d = q("SELECT COUNT(*) - COUNT(DISTINCT transaction_id) FROM fact_transactions")
    add("unique_transaction_id", d == 0, f"duplicates={d}")
    # 5. SCD2 integrity: exactly one current row per area, no overlapping ranges
    bad = q("SELECT COUNT(*) FROM (SELECT area_id FROM dim_area WHERE is_current = 1 "
            "GROUP BY area_id HAVING COUNT(*) <> 1) x")
    add("scd2_one_current_row", bad == 0, f"areas_violating={bad}")
    # 6. business-rule sanity: price per sqm in a plausible AED range
    out = q("SELECT COUNT(*) FROM fact_transactions WHERE price_per_sqm < 1000 OR price_per_sqm > 150000")
    add("price_per_sqm_range", out == 0, f"outliers={out}", warn=True)
    # 7. reject rate threshold
    rate = rejected / extracted if extracted else 0
    add("reject_rate_below_5pct", rate < 0.05, f"reject_rate={rate:.2%}", warn=True)

    pd.DataFrame(checks, columns=["run_id", "check_name", "status", "detail", "checked_at"]) \
        .to_sql("dq_log", engine, if_exists="append", index=False)
    for c in checks:
        (log.info if c[2] == "PASS" else log.warning)("DQ %-26s %s  %s", c[1], c[2], c[3])
    return all(c[2] != "FAIL" for c in checks)


# --------------------------------------------------------------------------
# export for Power BI (CSV import mode) + main
# --------------------------------------------------------------------------
def export_csv(engine, out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for t in ["dim_date", "dim_area", "dim_property", "dim_buyer_type",
              "dim_transaction_type", "fact_transactions", "dq_log", "etl_run_log"]:
        pd.read_sql(f"SELECT * FROM {t}", engine).to_csv(out / f"{t}.csv", index=False)
    log.info("star schema exported to %s", out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--transactions", required=True)
    ap.add_argument("--area-master", required=True)
    ap.add_argument("--db", default=f"sqlite:///{ROOT / 'output' / 'dubai_dw.db'}")
    ap.add_argument("--export", default=str(ROOT / "output" / "powerbi_export"))
    a = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    global engine
    (ROOT / 'output').mkdir(exist_ok=True)
    engine = create_engine(a.db)
    run_id, t0, started = uuid.uuid4().hex[:12], time.time(), now()
    log.info("run %s started", run_id)

    run_sql_file(engine, ROOT / "sql" / "schema.sql")
    raw, area_raw = extract(a.transactions), pd.read_csv(a.area_master)
    stage(engine, raw, area_raw, run_id)

    good, rejects, dupes = clean(raw, run_id)
    if len(rejects):
        rejects.to_sql("reject_transactions", engine, if_exists="append", index=False)
    log.info("clean: %s good, %s rejected, %s duplicates", f"{len(good):,}", f"{len(rejects):,}", f"{dupes:,}")

    load_dim_date(engine)
    new_a, chg_a = load_dim_area_scd2(engine, area_raw)
    log.info("dim_area SCD2: %d new, %d versioned (changed)", new_a, chg_a)
    n_p = load_simple_dim(engine, "dim_property", "property_key",
                          ["property_type", "rooms", "sale_stage"],
                          good.rename(columns={"prop_type_en": "property_type", "rooms_en": "rooms",
                                               "offplan": "sale_stage"}))
    n_b = load_simple_dim(engine, "dim_buyer_type", "buyer_type_key", ["buyer_type"], good)
    n_t = load_simple_dim(engine, "dim_transaction_type", "txn_type_key", ["txn_type"],
                          good.rename(columns={"trans_group_en": "txn_type"}))
    log.info("new dim members: property=%d buyer=%d txn_type=%d", n_p, n_b, n_t)

    loaded = load_fact(engine, good, run_id)
    log.info("fact_transactions: %s new rows loaded", f"{loaded:,}")

    ok = run_dq(engine, run_id, len(raw), len(rejects), dupes, loaded)
    run_sql_file(engine, ROOT / "sql" / "views.sql")

    with engine.begin() as con:
        con.execute(text("DELETE FROM etl_state WHERE pipeline = 'transactions'"))
        con.execute(text("INSERT INTO etl_state VALUES ('transactions', :d)"),
                    {"d": good["txn_date"].max().strftime("%Y-%m-%d")})
        con.execute(text("INSERT INTO etl_run_log VALUES (:r,:s,:f,:e,:rj,:d,:l,:st,:t)"),
                    {"r": run_id, "s": started, "f": now(), "e": len(raw), "rj": len(rejects),
                     "d": dupes, "l": loaded, "st": "SUCCESS" if ok else "FAILED",
                     "t": round(time.time() - t0, 2)})
    export_csv(engine, a.export)
    log.info("run %s finished in %.1fs  status=%s", run_id, time.time() - t0, "SUCCESS" if ok else "FAILED")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
