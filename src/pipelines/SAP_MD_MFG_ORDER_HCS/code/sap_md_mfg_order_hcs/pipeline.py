from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_mfg_order_hcs.config.ConfigStore import *
from sap_md_mfg_order_hcs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_mfg_order_hcs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_AUFK = AUFK(spark)
    df_DELETED = DELETED(spark, df_AUFK)
    df_XFORM = XFORM(spark, df_DELETED)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    MFG_ORDER(spark, df_SELECT_FIELDS)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MFG_ORDER_HCS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MFG_ORDER_HCS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
