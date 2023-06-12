from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_mbp_fsn.config.ConfigStore import *
from sap_md_co_cd_mbp_fsn.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_mbp_fsn.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_HMD_KNB1 = DS_SAP_HMD_KNB1(spark)
    df_DS_SAP_HMD_KNB1 = collectMetrics(
        spark, 
        df_DS_SAP_HMD_KNB1, 
        "graph", 
        "emiI_yom1y6w5OjRkfMZW$$ZMqLmCzq8O498Gp3kJimy", 
        "FdFYmWVcZxCjUJUfKIlR2$$ZZHwMIiDgmx18PNdZ-g3M"
    )
    df_MANDT_FILTER_KNB1 = MANDT_FILTER_KNB1(spark, df_DS_SAP_HMD_KNB1)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER_KNB1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "KDnE4a3-i7i8jPR9w8ps-$$v7WSsnis3v8FUXGyanHmR", 
        "EfrICXb1yucfi2O7H7p7Z$$qd1GWza0gBJifhXb1_vnw"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    MD_CO_CD(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "ckYKqaK_sIrAq_4dexM71$$RrIWmR4CXEp0fF8H849BG", 
        "_2wc4qJwyLuTK9QeRrKBA$$ToYdv4VZKOpQw3HYI2IjS"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_MBP_FSN_ATL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_MBP_FSN_ATL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
