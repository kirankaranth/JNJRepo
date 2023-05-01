from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_sap_matl_uom_mbp_svs_p01_hcs_hmd.config.ConfigStore import *
from md_sap_matl_uom_mbp_svs_p01_hcs_hmd.udfs.UDFs import *
from prophecy.utils import *
from md_sap_matl_uom_mbp_svs_p01_hcs_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_02_T006A = DS_SAP_02_T006A(spark)
    df_DS_SAP_02_T006A = collectMetrics(
        spark, 
        df_DS_SAP_02_T006A, 
        "graph", 
        "33Si4hyqOhKKicZnQDxYt$$l7mlQPr8ne5y_3m1lSO6F", 
        "Yivug2nXrHM2IPmYHiuwj$$3kmglUf6LyJUniOE_bj4e"
    )
    df_MANDT_02 = MANDT_02(spark, df_DS_SAP_02_T006A)
    Lookup_02_T006A(spark, df_MANDT_02)
    Lookup_03_T006A(spark, df_MANDT_02)
    Lookup_01_T006A(spark, df_MANDT_02)
    Lookup_04_T006A(spark, df_MANDT_02)
    df_DS_SAP_01_T006 = DS_SAP_01_T006(spark)
    df_DS_SAP_01_T006 = collectMetrics(
        spark, 
        df_DS_SAP_01_T006, 
        "graph", 
        "Y-Hh-6PcWPPAEb9J8JunT$$6btFRh78MOylBtZCiHKjH", 
        "IwYzuN1uBduf6FyuL4nev$$AMPfxC2YXYj-z-unYRj40"
    )
    df_MANDT_01 = MANDT_01(spark, df_DS_SAP_01_T006)
    df_NEW_FIELDS_01_T006 = NEW_FIELDS_01_T006(spark, df_MANDT_01)
    df_SET_FIELD_ORDER = SET_FIELD_ORDER(spark, df_NEW_FIELDS_01_T006)
    df_SET_FIELD_ORDER = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER, 
        "graph", 
        "x-6JBrO04IxLmZcoH3-Lo$$_VTNKVy4oEqhnc6hx2zGV", 
        "5z7RZEjaMwuq8IGz3-zbd$$mG4m-0Rot8gJKOv98_4oq"
    )
    MD_MATL_UOM(spark, df_SET_FIELD_ORDER)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SAP_MATL_UOM_ATL_BBA_BBL_BBN_BWI_FSN_GEU_MRS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SAP_MATL_UOM_ATL_BBA_BBL_BBN_BWI_FSN_GEU_MRS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
