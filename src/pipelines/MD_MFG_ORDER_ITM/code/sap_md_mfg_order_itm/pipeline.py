from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_mfg_order_itm.config.ConfigStore import *
from sap_md_mfg_order_itm.udfs.UDFs import *
from prophecy.utils import *
from sap_md_mfg_order_itm.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_AFPO = SAP_AFPO(spark)
    df_SAP_AFPO = collectMetrics(
        spark, 
        df_SAP_AFPO, 
        "graph", 
        "Hc2tfTvUG4o-CD4G4r944$$aRgAJgTFygecIVANkMxcY", 
        "K130vKiIxyxRxxMsjhdWo$$ENaVa2Wysxhy81tkLZ2k3"
    )
    df_MANDT_FILTER_AFPO = MANDT_FILTER_AFPO(spark, df_SAP_AFPO)
    df_SAP_AUFK = SAP_AUFK(spark)
    df_SAP_AUFK = collectMetrics(
        spark, 
        df_SAP_AUFK, 
        "graph", 
        "w0b2EBi6fYDOqAY92Vs05$$hikvZb9p-TDLwtD_Wgifg", 
        "MXttMvFvDRZApXar_L5NP$$577DSWMImYLVOa8yFbxmE"
    )
    df_MANDT_FILTER_AUFK = MANDT_FILTER_AUFK(spark, df_SAP_AUFK)
    df_SELECT_AUFK = SELECT_AUFK(spark, df_MANDT_FILTER_AUFK)
    df_JOIN = JOIN(spark, df_MANDT_FILTER_AFPO, df_SELECT_AUFK)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_JOIN)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "CZweoIczBjB9GmmBUdy-_$$uOt9w0ER19a6_-qqslvfz", 
        "EKzD0dzFYn7p6lbSsFX2Y$$URiXs1GhaIm_UJVLgPnSu"
    )
    MD_MFG_ORDER_ITM(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "ckzCQHpZRR7WvR_PmTd2n$$sv7xW_tthgI47SuHqP4pm", 
        "MlerYofX13bBa2GID2NR3$$ED_Lc3Ukk0324Z1QOGD37"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_MFG_ORDER_ITM")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_MFG_ORDER_ITM")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
