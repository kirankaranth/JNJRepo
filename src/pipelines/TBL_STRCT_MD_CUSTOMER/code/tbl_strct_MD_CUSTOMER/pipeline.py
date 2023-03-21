from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_CUSTOMER.config.ConfigStore import *
from tbl_strct_MD_CUSTOMER.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_CUSTOMER.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_CUST_SLS_PTNR = sql_MD_CUST_SLS_PTNR(spark)
    MD_CUST_SLS_PTNR(spark, df_sql_MD_CUST_SLS_PTNR)
    df_sql_MD_CUST_MSTR_UNLD_DATA = sql_MD_CUST_MSTR_UNLD_DATA(spark)
    MD_CUST_MSTR_UNLD_DATA(spark, df_sql_MD_CUST_MSTR_UNLD_DATA)
    df_sql_MD_EMAIL_ADDR = sql_MD_EMAIL_ADDR(spark)
    MD_EMAIL_ADDR(spark, df_sql_MD_EMAIL_ADDR)
    df_sql_MD_CUST_SLS_AREA = sql_MD_CUST_SLS_AREA(spark)
    df_sql_MD_CUST_CR = sql_MD_CUST_CR(spark)
    MD_CUST_CR(spark, df_sql_MD_CUST_CR)
    df_sql_MD_CO_CD = sql_MD_CO_CD(spark)
    df_sql_MD_CUST = sql_MD_CUST(spark)
    df_sql_MD_CUST_HIER = sql_MD_CUST_HIER(spark)
    MD_CUST_HIER(spark, df_sql_MD_CUST_HIER)
    MD_CUST(spark, df_sql_MD_CUST)
    MD_CO_CD(spark, df_sql_MD_CO_CD)
    MD_CUST_SLS_AREA(spark, df_sql_MD_CUST_SLS_AREA)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_CUSTOMER")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_CUSTOMER")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
