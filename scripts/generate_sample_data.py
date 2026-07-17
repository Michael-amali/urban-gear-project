import json, random, argparse
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker
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

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--records-per-day", type=int, default=20000)
    p.add_argument("--bad-rate", type=float, default=0.07)
    p.add_argument("--output", default="data/raw")
    args = p.parse_args()

    out_base = Path(args.output)
    start = datetime.now() - timedelta(days=args.days)

    for i in range(args.days):
        dt = (start + timedelta(days=i)).replace(hour=10, minute=0, second=0)
        date_str = dt.strftime("%Y-%m-%d")
        folder = out_base / f"orders/dt={date_str}"
        folder.mkdir(parents=True, exist_ok=True)
        file_path = folder / f"part-00000-{date_str}.json"
        print(f"Generating {args.records_per_day} for {date_str} -> {file_path}")
        with open(file_path, "w") as f:
            for _ in range(args.records_per_day):
                f.write(json.dumps(gen_record(dt, args.bad_rate)) + "\n")
        # Add corrupt json line
        with open(file_path, "a") as f:
            f.write('{"order_id": "BAD_JSON", "quantity": }\n')

    print("Done")

if __name__ == "__main__":
    main()