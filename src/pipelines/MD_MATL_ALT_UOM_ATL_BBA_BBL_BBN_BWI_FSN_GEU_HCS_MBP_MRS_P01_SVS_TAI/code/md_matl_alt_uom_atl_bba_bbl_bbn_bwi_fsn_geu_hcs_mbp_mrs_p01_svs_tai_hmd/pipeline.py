from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.config.ConfigStore import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.udfs.UDFs import *
from prophecy.utils import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_MARA_01 = DS_SAP_MARA_01(spark)
    df_DS_SAP_MARA_01 = collectMetrics(
        spark, 
        df_DS_SAP_MARA_01, 
        "graph", 
        "61L0HnhccZW2gq1xdCSDI$$5LB3_g6JdscOYsQolGbBQ", 
        "DTIlKMzDvq6FPJHIrBbud$$Vps9ShjmHDHlIzJf5Nk_h"
    )
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_DS_SAP_MARA_01)
    df_MANDT1 = MANDT1(spark, df_SELECT_FIELDS)
    MEINS_LU(spark, df_MANDT1)
    df_DS_SAP_01_MARM = DS_SAP_01_MARM(spark)
    df_DS_SAP_01_MARM = collectMetrics(
        spark, 
        df_DS_SAP_01_MARM, 
        "graph", 
        "taWajNbWifZpohozbS4St$$CxbiIVPI2kjSyw5BmEUjF", 
        "y21rOpvx1-CugFPWmLB2T$$y4kQ_YJ_qRlhC4PV76-Wc"
    )
    df_MANDT = MANDT(spark, df_DS_SAP_01_MARM)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_MANDT)
    df_SET_FIELDS = SET_FIELDS(spark, df_SchemaTransform_1)
    df_SET_FIELDS = collectMetrics(
        spark, 
        df_SET_FIELDS, 
        "graph", 
        "LnMkjaXjwHhLl2OUtniJk$$kQU8TmqGjEn4rFOvwzxp1", 
        "CXsK6Edgczs48PrXz0o_V$$qaKNGtqOJLjAxJSg1eRYA"
    )
    MD_MATL_ALT_UOM(spark, df_SET_FIELDS)

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
    spark.conf.set(
        "prophecy.metadata.pipeline.uri",
        "pipelines/MD_MATL_ALT_UOM_ATL_BBA_BBL_BBN_BWI_FSN_GEU_HCS_MBP_MRS_P01_SVS_TAI"
    )
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/MD_MATL_ALT_UOM_ATL_BBA_BBL_BBN_BWI_FSN_GEU_HCS_MBP_MRS_P01_SVS_TAI"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
