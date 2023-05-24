from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_hcs.config.ConfigStore import *
from sap_md_matl_hcs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_hcs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_T134T = T134T(spark)
    df_T134T = collectMetrics(
        spark, 
        df_T134T, 
        "graph", 
        "w1_Ig8YgEF05yZXQKs90v$$qM7G7Ftnv349dUEIO7PK2", 
        "ZukjsOrRWY5Xxl8c1dhWu$$1Vb4qrrfYBcwUhM_m5ZV1"
    )
    df_DEL_MANDT_5 = DEL_MANDT_5(spark, df_T134T)
    MTBEZ_LU(spark, df_DEL_MANDT_5)
    df_T023T = T023T(spark)
    df_T023T = collectMetrics(
        spark, 
        df_T023T, 
        "graph", 
        "5MRcFhgyxpf0aWdf4BSfl$$QRm6-j7Kwyf0l78lGGJHi", 
        "Gfxq4jmjiR9kmXeE3uLmj$$Ho_buRxciJNsR2ALfA_UA"
    )
    df_DEL_MANDT_9 = DEL_MANDT_9(spark, df_T023T)
    WGBEZx_LU(spark, df_DEL_MANDT_9)
    df_MAKT = MAKT(spark)
    df_MAKT = collectMetrics(
        spark, 
        df_MAKT, 
        "graph", 
        "CVd3sveEgEBO0q-uLezyN$$tQjh2y_tGilSTIBcLgEwl", 
        "3iIbwIlsIP-g2-BYTzQKP$$rEZVdwZX6etMmxhWhKQBt"
    )
    df_DEL_MANDT1 = DEL_MANDT1(spark, df_MAKT)
    MAKTX_LU(spark, df_DEL_MANDT1)
    df_MARA = MARA(spark)
    df_MARA = collectMetrics(
        spark, 
        df_MARA, 
        "graph", 
        "xusvnsXk7hFQx0eRCrSBg$$YaaRM2iukmcmd4NYXj0oV", 
        "hPgxdnXRYnNB6b7fg2o9F$$WGnXaUSKhHR_hJlwYyHs_"
    )
    df_DEL_MANDT = DEL_MANDT(spark, df_MARA)
    df_XFORM = XFORM(spark, df_DEL_MANDT)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_HCS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_HCS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
