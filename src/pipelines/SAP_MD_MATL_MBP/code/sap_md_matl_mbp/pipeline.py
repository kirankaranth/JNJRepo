from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_mbp.config.ConfigStore import *
from sap_md_matl_mbp.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_mbp.graph import *

def pipeline(spark: SparkSession) -> None:
    df_MAKT = MAKT(spark)
    df_MAKT = collectMetrics(
        spark, 
        df_MAKT, 
        "graph", 
        "CVd3sveEgEBO0q-uLezyN$$tQjh2y_tGilSTIBcLgEwl", 
        "3iIbwIlsIP-g2-BYTzQKP$$rEZVdwZX6etMmxhWhKQBt"
    )
    df_DEL_AND_MANDT_1 = DEL_AND_MANDT_1(spark, df_MAKT)
    MAKTX_LU(spark, df_DEL_AND_MANDT_1)
    df_T134T = T134T(spark)
    df_T134T = collectMetrics(
        spark, 
        df_T134T, 
        "graph", 
        "COTcRAZ-5v4aQt3U28lV6$$Nzn0z9EaQTkDn1wOv9zyZ", 
        "aNafwnpniB8sZ69oOACq1$$3Kzs8ETmn2qVBCgcUNRVO"
    )
    df_DEL_MANDT_5 = DEL_MANDT_5(spark, df_T134T)
    MTBEZ_LU(spark, df_DEL_MANDT_5)
    df_T023T = T023T(spark)
    df_T023T = collectMetrics(
        spark, 
        df_T023T, 
        "graph", 
        "hxOOb1oDSnbY_K5ptRNOR$$Jhpya6d8wN-ElOLd4xl6l", 
        "O3cPoUk-r_CDnUplqSSyf$$Uceeq9vsXFwcZrZsOSym0"
    )
    df_DEL_MANDT_9 = DEL_MANDT_9(spark, df_T023T)
    WGBEZx_LU(spark, df_DEL_MANDT_9)
    df_MARA_MBP = MARA_MBP(spark)
    df_MARA_MBP = collectMetrics(
        spark, 
        df_MARA_MBP, 
        "graph", 
        "WacNkgb_YDehtFiCRhh6z$$jPjL08HaHvzBdVE8WW3w4", 
        "3u-su5zdIcRsm1X95dIh1$$P8Jz4YwGJGDs9Q3cyuBzs"
    )
    df_DEL_AND_MANDT = DEL_AND_MANDT(spark, df_MARA_MBP)
    df_XFORM = XFORM(spark, df_DEL_AND_MANDT)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    df_SELECT_FIELDS = collectMetrics(
        spark, 
        df_SELECT_FIELDS, 
        "graph", 
        "5tjEC2R_fitFg858EKn_H$$8zrgDqoYbVtMNO-GK4V6_", 
        "bPAwoIg9wa3XArlXP-7m_$$c1QUVrlfKTD9A8sObAKfS"
    )
    TARGET(spark, df_SELECT_FIELDS)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_MBP")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_MBP")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
