import pytest
import os
from pyspark.context import SparkContext
from awsglue.context import GlueContext


@pytest.fixture(scope="session", autouse=True)
def scope_session():
    # テスト内で使い回せるようにS3のURLを環境変数に設定
    os.environ["TEST_S3_ENDPOINT_URL"] = "http://aws.local:4572"
    sc = SparkContext()
    # S3のエンドポイントをLocalStackへ差し替える
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://aws.local:4572")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.signing-algorithm", "S3SignerType")
    pytest.sc = sc
    pytest.glueContext = GlueContext(pytest.sc)
    pytest.spark = pytest.glueContext.spark_session
    # JOINで作成したDataFrameを操作しようとするとエラーになるので設定追加
    pytest.spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)
