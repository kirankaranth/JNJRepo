from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_DOC.config.ConfigStore import *
from tbl_strct_MD_DOC.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_DOC.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DOC_HDR_INVC_RCPT = sql_MD_DOC_HDR_INVC_RCPT(spark)
    MD_DOC_HDR_INVC_RCPT(spark, df_sql_MD_DOC_HDR_INVC_RCPT)
    df_sql_MD_DOC_ITM_INCM_INVC = sql_MD_DOC_ITM_INCM_INVC(spark)
    MD_DOC_ITM_INCM_INVC(spark, df_sql_MD_DOC_ITM_INCM_INVC)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_DOC")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_DOC")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
