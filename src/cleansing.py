from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def drop_dup_keep_latest(spark: SparkSession, df: DataFrame,
                         id_col: str, date_col: str,
                         keep_date_null: bool) -> DataFrame:
    df.createOrReplaceTempView('dataset')
    add_cond = ''
    if keep_date_null:
        add_cond = 'OR (T2.{0} IS NULL AND T1.{0} IS NULL)'.format(date_col)
    query = '''
      SELECT
        T1.*
      FROM dataset AS T1
      INNER JOIN (
        SELECT
          {id_col},
          MAX({date_col}) AS {date_col}
        FROM dataset GROUP BY {id_col}
      ) AS T2
      ON T2.{id_col} = T1.{id_col}
      AND (T2.{date_col} = T1.{date_col} {add_cond})
      ORDER BY {id_col}
    '''.format(id_col=id_col, date_col=date_col, add_cond=add_cond)

    print(query)
    df = spark.sql(query)
    spark.catalog.dropTempView('dataset')
    return df
