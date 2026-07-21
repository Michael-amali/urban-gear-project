import json, random, argparse, sys, tempfile, os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from faker import Faker
from config.pipeline_config import PipelineConfig, VALID_ENVS
from config.s3_client import get_s3_client

fake = Faker()
Faker.seed(0)
random.seed(0)

PLATFORMS = ["website", "mobile_app", "marketplace"]
CATEGORIES = ["Shoes", "Apparel", "Accessories", "Gear"]
PAYMENTS = ["CREDIT_CARD", "PAYPAL", "APPLE_PAY", "DEBIT"]

def gen_record(dt: datetime, bad_rate=0.05):
    is_bad = random.random() < bad_rate
    bad_type = random.choice(["neg_qty", "future", "null_customer", "null_qty"]) if is_bad else None

    base = {
        "order_id": f"ORD-{fake.uuid4()[:8]}",
        "customer_id": fake.uuid4()[:8] if bad_type != "null_customer" else None,
        "platform": random.choice(PLATFORMS),
        "order_date": dt.isoformat() if bad_type != "future" else (dt + timedelta(days=5)).isoformat(),
        "product_id": f"PROD-{random.randint(1000,9999)}",
        "product_name": fake.word().title(),
        "category": random.choice(CATEGORIES),
        "quantity": -random.randint(1,5) if bad_type == "neg_qty" else random.randint(1,5) if bad_type != "null_qty" else None,
        "unit_price": round(random.uniform(20, 500), 2),
        "discount_pct": random.choice([0,0,5,10,15]),
        "shipping_cost": round(random.uniform(0,20),2),
        "tax_amount": round(random.uniform(0,30),2),
        "payment_method": random.choice(PAYMENTS),
        "shipping_country": "USA",
        "shipping_state": fake.state_abbr(),
        "shipping_city": fake.city(),
        "is_returned": random.random() < 0.05
    }
    return base

def _write_day_records(file_handle, dt, records_per_day, bad_rate):
    for _ in range(records_per_day):
        file_handle.write(json.dumps(gen_record(dt, bad_rate)) + "\n")
    file_handle.write('{"order_id": "BAD_JSON", "quantity": }\n')


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--records-per-day", type=int, default=20000)
    p.add_argument("--bad-rate", type=float, default=0.07)
    p.add_argument("--output", default="data/raw")
    p.add_argument(
        "--env",
        default=os.getenv("ENV", "local"),
        choices=list(VALID_ENVS),
        help="Storage backend: local (filesystem), local-s3 (MinIO), prod (AWS S3)",
    )
    args = p.parse_args()

    PipelineConfig.reload(env=args.env)
    start = datetime.now() - timedelta(days=args.days)

    s3 = None
    bucket = None
    if PipelineConfig.use_object_storage():
        s3 = get_s3_client()
        bucket = PipelineConfig.S3_BUCKET

    for i in range(args.days):
        dt = (start + timedelta(days=i)).replace(hour=10, minute=0, second=0)
        date_str = dt.strftime("%Y-%m-%d")

        if PipelineConfig.use_local_fs():
            out_base = Path(args.output)
            folder = out_base / f"orders/dt={date_str}"
            folder.mkdir(parents=True, exist_ok=True)
            file_path = folder / f"part-00000-{date_str}.json"
            print(f"Generating {args.records_per_day} for {date_str} -> {file_path}")
            with open(file_path, "w", encoding="utf-8") as f:
                _write_day_records(f, dt, args.records_per_day, args.bad_rate)
        else:
            key = f"raw/orders/dt={date_str}/part-00000-{date_str}.json"
            print(f"Generating {args.records_per_day} for {date_str} -> s3://{bucket}/{key}")
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
                _write_day_records(f, dt, args.records_per_day, args.bad_rate)
                tmp_path = f.name
            s3.upload_file(tmp_path, bucket, key)
            Path(tmp_path).unlink(missing_ok=True)
            print(f"  uploaded s3://{bucket}/{key}")

    print(f"Done (ENV={PipelineConfig.ENV})")

if __name__ == "__main__":
    main()
