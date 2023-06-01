from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hmd_mbp_mrs_p01_svs.config.ConfigStore import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hmd_mbp_mrs_p01_svs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hmd_mbp_mrs_p01_svs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_AUFK = AUFK(spark)
    df_AUFK = collectMetrics(
        spark, 
        df_AUFK, 
        "graph", 
        "GzZy7epkAeEPnPhOFfikV$$T_bzHkqtp5Smicsxv8Jkz", 
        "clWL8zK7XPUMMzoNesnYP$$udBwoK-r9Ye8eLCJNWiys"
    )
    df_DELETED = DELETED(spark, df_AUFK)
    df_AFKO = AFKO(spark)
    df_AFKO = collectMetrics(
        spark, 
        df_AFKO, 
        "graph", 
        "FCDBOZBZXar7KymMTlm4O$$J50rSlMNbpsLRLUZDiAjN", 
        "jbKCg95LJ4LOJ7xfLre7Y$$xSqgN69N8Wc2MPZ7BoNx7"
    )
    df_DEL3 = DEL3(spark, df_AFKO)
    df_RE_NAME = RE_NAME(spark, df_DEL3)
    df_TJ02T = TJ02T(spark)
    df_TJ02T = collectMetrics(
        spark, 
        df_TJ02T, 
        "graph", 
        "7svcEXBKbohvqy5A5pht3$$xMWyjdHkfQZr6NScVn0zS", 
        "RYBbWOa5v9AIGLycbJhkc$$NbJhUFh6lM5C-Te0wzvTz"
    )
    df_DEL_SPRAS = DEL_SPRAS(spark, df_TJ02T)
    df_JEST = JEST(spark)
    df_JEST = collectMetrics(
        spark, 
        df_JEST, 
        "graph", 
        "RZ2nci0oJbC0JZGnFCVGV$$ZZ2zxj_8jW1f0XxEBfXQi", 
        "OGKh5A-lI-a_YKvwiHhgn$$3xHIQxG8AB_UkNcfWqy64"
    )
    df_DEL2 = DEL2(spark, df_JEST)
    df_STAT_TXT = STAT_TXT(spark, df_DEL2, df_DEL_SPRAS)
    df_LIST_STATUS = LIST_STATUS(spark, df_STAT_TXT)
    df_JOIN = JOIN(spark, df_DELETED, df_RE_NAME, df_LIST_STATUS)
    df_XFORM = XFORM(spark, df_JOIN)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    df_SELECT_FIELDS = collectMetrics(
        spark, 
        df_SELECT_FIELDS, 
        "graph", 
        "Py1rJC4RtV-csVEyU8nnU$$QrM1ueusjZ7l3UeQpuI4Q", 
        "K-eB0RQLMTcLM4fX2_eRq$$8tlrxU6k4GqR21Nj78679"
    )
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
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MFG_ORDER_ATL_BBL_BBN_BWI_HM2_HMD_MBP_MRS_P01_SVS")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/SAP_MD_MFG_ORDER_ATL_BBL_BBN_BWI_HM2_HMD_MBP_MRS_P01_SVS"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
