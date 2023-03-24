from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_BUSINESS_PARTNER.config.ConfigStore import *
from tbl_strct_MD_BUSINESS_PARTNER.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_BUSINESS_PARTNER.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_BUSN_PTNR = sql_MD_BUSN_PTNR(spark)
    MD_BUSN_PTNR(spark, df_sql_MD_BUSN_PTNR)
    df_sql_MD_BUSN_PTNR_ROLES = sql_MD_BUSN_PTNR_ROLES(spark)
    MD_BUSN_PTNR_ROLES(spark, df_sql_MD_BUSN_PTNR_ROLES)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_BUSINESS_PARTNER")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_BUSINESS_PARTNER")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
