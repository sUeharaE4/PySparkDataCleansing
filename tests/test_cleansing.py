import pytest
import boto3
import json
import os
import sys
import io
import csv
from src.cleansing import drop_dup_keep_latest
from botocore.client import Config
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql import functions


@pytest.mark.parametrize('tsv_path, csv_schema_path, json_schema_path, \
                          id_col, date_col, keep_date_null', [
    ('input/cleansing_base/data/001.tsv',
     'input/cleansing_base/schema/001_csv.json',
     'input/cleansing_base/schema/001_json.json',
     'data_id',
     'datetime',
     True),
    ('input/cleansing_base/data/001.tsv',
     'input/cleansing_base/schema/001_csv.json',
     'input/cleansing_base/schema/001_json.json',
     'data_id',
     'datetime',
     False),
])
def test_drop_dup_keep_latest(tsv_path, csv_schema_path, json_schema_path,
                              id_col, date_col, keep_date_null):
    # setup
    spark = pytest.spark
    pwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(pwd, csv_schema_path)) as f:
        csv_schema = StructType.fromJson(json.load(f))
    with open(os.path.join(pwd, json_schema_path)) as f:
        json_schema = ArrayType.fromJson(json.load(f))

    df = spark.read.csv(os.path.join(pwd, tsv_path),
                        header=True, sep='\t', schema=csv_schema)
    df = df.withColumn('tmp_payload',
                       functions.explode(functions.from_json(
                           functions.col('payload'), json_schema)))
    df = df.withColumn('status', functions.col('tmp_payload.status')) \
           .withColumn('is_old', functions.col('tmp_payload.is_old')) \
           .withColumn('order_date', functions.col('tmp_payload.order_date')) \
           .withColumn('timestamp', functions.col('tmp_payload.timestamp')) \
           .drop('payload', 'tmp_payload')

    # exec
    tmp_df = df.groupBy(id_col).agg(functions.max(date_col).alias(date_col))\
               .sort(id_col)
    if not keep_date_null:
        tmp_df = tmp_df.dropna(subset=date_col)
    res_df = drop_dup_keep_latest(pytest.spark, df, id_col, date_col, keep_date_null)

    # assert
    ans = [list(row) for row in tmp_df.collect()]
    res = [list(row) for row in res_df.select(id_col, date_col).collect()]
    assert res == ans
