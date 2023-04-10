from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_bom.config.ConfigStore import *
from sap_md_matl_bom.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_bom.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_MAST = SAP_MAST(spark)
    df_Filter_SAP_01_MAST = Filter_SAP_01_MAST(spark, df_SAP_MAST)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Filter_SAP_01_MAST)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    MD_MATL_BOM(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_BOM")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_BOM")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
