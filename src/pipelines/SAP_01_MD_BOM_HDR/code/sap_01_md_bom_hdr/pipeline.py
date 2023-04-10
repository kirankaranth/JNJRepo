from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_bom_hdr.config.ConfigStore import *
from sap_01_md_bom_hdr.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_bom_hdr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_STKO = SAP_STKO(spark)
    df_STKO_MANDT_FILTER = STKO_MANDT_FILTER(spark, df_SAP_STKO)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_STKO_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    MD_BOM_HDR(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_BOM_HDR")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_BOM_HDR")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
