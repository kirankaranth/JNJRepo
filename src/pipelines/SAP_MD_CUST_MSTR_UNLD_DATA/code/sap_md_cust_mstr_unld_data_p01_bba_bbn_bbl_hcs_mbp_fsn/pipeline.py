from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.config.ConfigStore import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.udfs.UDFs import *
from prophecy.utils import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_KNVA = DS_SAP_01_KNVA(spark)
    df_DS_SAP_01_KNVA = collectMetrics(
        spark, 
        df_DS_SAP_01_KNVA, 
        "graph", 
        "TMF5Djt6miPQql-8wNqAA$$gZKwcXaQfs44GtbtFCD1u", 
        "Coq9u8jWDsY9LA1NDkEYJ$$i2B5wpS-3dxo7wZ3hUytL"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_01_KNVA)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_MD_NEWTABLE_SWAP = MD_NEWTABLE_SWAP(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_MD_NEWTABLE_SWAP = collectMetrics(
        spark, 
        df_MD_NEWTABLE_SWAP, 
        "graph", 
        "vBhzurb-xJkeINiB2EmXB$$mKRdksHkBOPw97QmtcJYf", 
        "XbwN5URqrTrGDr1p0gplR$$rsPhQkgHzdY4TWfjsXsR8"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_MD_NEWTABLE_SWAP)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "Hw1ckxxsmt2ywCWlICpT2$$SS6JlfS3w5tlE9Q4bd3-4", 
        "GMXoEgJIal0b3CaFBs8rl$$E2C4uSOZCfNE44GAVqM-9"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
    MD_CUST_MSTR_UNLD_DATA(spark, df_MD_NEWTABLE_SWAP)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CUST_MSTR_UNLD_DATA")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CUST_MSTR_UNLD_DATA")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
