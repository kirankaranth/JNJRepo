from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_mfg_order_itm.config.ConfigStore import *
from jde_md_mfg_order_itm.udfs.UDFs import *
from prophecy.utils import *
from jde_md_mfg_order_itm.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F4801 = DS_JDE_01_F4801(spark)
    df_MANDT_FILTER_AFPO = MANDT_FILTER_AFPO(spark, df_DS_JDE_01_F4801)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER_AFPO)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    MD_MFG_ORDER_ITM(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MFG_ORDER_ITM")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MFG_ORDER_ITM")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
