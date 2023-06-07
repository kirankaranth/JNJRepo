from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_po_sched_line_delv_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_hmd_mbp_mrs_p01_svs_tai_hmd.config.ConfigStore import *
from sap_md_po_sched_line_delv_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_hmd_mbp_mrs_p01_svs_tai_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_po_sched_line_delv_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_hmd_mbp_mrs_p01_svs_tai_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_EKET = SAP_EKET(spark)
    df_SAP_EKET = collectMetrics(
        spark, 
        df_SAP_EKET, 
        "graph", 
        "Q1P0IN4l3UDlGcwGyUEqs$$zrz-joBwKJI_-G3uJEe4t", 
        "aRRzPl8nFgkS1whLvR3bR$$GedUo5lSg-1C_2qv4L5cu"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_EKET)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "OEaeNUW_RvrM9cHfAc11x$$29C477ZRboEnYjZt87b0X", 
        "9RI0M3Il1xoJLRuN0kTxu$$PSf7M9SDfj236P1lcxAKC"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "jv8w4-b8lAUofwLgqZsmH$$aZwzuVCtC4tBiR6T5j-k2", 
        "JE317dE5oWSrT07ZzKzLf$$wQSdgRJj6QFrB8SULV4v5"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
    MD_PO_SCHED_LINE_DELV(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_PO_SCHED_LINE_DELV")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_PO_SCHED_LINE_DELV")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
