from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MES_MD_REASON_CODES.config.ConfigStore import *
from tbl_strct_MES_MD_REASON_CODES.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MES_MD_REASON_CODES.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MES_MD_LOSS_RSN = sql_MES_MD_LOSS_RSN(spark)
    df_sql_MES_MD_BNS_RSN = sql_MES_MD_BNS_RSN(spark)
    df_sql_MES_MD_RWRK_RSN = sql_MES_MD_RWRK_RSN(spark)
    df_sql_MES_MD_ISS_DIFF_RSN = sql_MES_MD_ISS_DIFF_RSN(spark)
    df_sql_MES_MD_CNTNR_DEFCT_RSN = sql_MES_MD_CNTNR_DEFCT_RSN(spark)
    df_sql_MES_MD_HOLD_RSN = sql_MES_MD_HOLD_RSN(spark)
    MES_MD_CNTNR_DEFCT_RSN(spark, df_sql_MES_MD_CNTNR_DEFCT_RSN)
    MES_MD_ISS_DIFF_RSN(spark, df_sql_MES_MD_ISS_DIFF_RSN)
    MES_MD_LOSS_RSN(spark, df_sql_MES_MD_LOSS_RSN)
    MES_MD_BNS_RSN(spark, df_sql_MES_MD_BNS_RSN)
    MES_MD_RWRK_RSN(spark, df_sql_MES_MD_RWRK_RSN)
    MES_MD_HOLD_RSN(spark, df_sql_MES_MD_HOLD_RSN)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MES_MD_REASON_CODES")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MES_MD_REASON_CODES")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
