import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from datetime import datetime
import pytest

from dotenv import load_dotenv
load_dotenv()


# @pytest.fixture(scope="session")
# def spark():
#     return SparkSession.builder.master("local[2]").appName("test").config("spark.sql.shuffle.partitions","2").getOrCreate()



@pytest.fixture(scope="session")
def spark():

    os.environ["PYSPARK_PYTHON"] =  os.getenv("PYTHON_PATH_311")
    os.environ["PYSPARK_DRIVER_PYTHON"] = os.getenv("PYTHON_PATH_311")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_negative_qty_rejected(spark):
    from etl.etl_job import tag_and_route, RAW_ORDER_SCHEMA
    data = [("o1","c1","website",datetime.now(),"p1","Prod","Cat",-5,10.0,0.0,0.0,0.0,"CARD","US","CA","LA",False,None)]
    df = spark.createDataFrame(data, schema=RAW_ORDER_SCHEMA)
    clean, rej = tag_and_route(df, "2026-06-21")
    assert clean.count()==0 and rej.count()==1
    assert rej.first()["rejection_reason"]=="negative_quantity"


def test_future_date_rejected(spark):
    from etl.etl_job import tag_and_route, RAW_ORDER_SCHEMA
    from datetime import timedelta
    future = datetime.now() + timedelta(days=5)
    data = [("o2","c1","website",future,"p1","Prod","Cat",1,10.0,0.0,0.0,0.0,"CARD","US","CA","LA",False,None)]
    df = spark.createDataFrame(data, schema=RAW_ORDER_SCHEMA)
    clean, rej = tag_and_route(df, "2026-06-21")
    assert rej.count()==1
    

def test_transform_revenue(spark):
    from etl.etl_job import tag_and_route, transform_clean, RAW_ORDER_SCHEMA
    data = [("o3","c1","website",datetime.now(),"p1","Prod","Cat",2,100.0,10.0,5.0,10.0,"CARD","US","CA","LA",False,None)]
    df = spark.createDataFrame(data, schema=RAW_ORDER_SCHEMA)
    clean,_ = tag_and_route(df, "2026-06-21")
    trans = transform_clean(clean, "2026-06-21")
    row = trans.first()
    assert row["gross_revenue"]==200.0
    assert row["net_revenue"]==180.0