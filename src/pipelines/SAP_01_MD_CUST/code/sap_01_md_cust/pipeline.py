from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_cust.config.ConfigStore import *
from sap_01_md_cust.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_cust.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_KNA1 = SAP_KNA1(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_KNA1)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_CUST")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_CUST")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
