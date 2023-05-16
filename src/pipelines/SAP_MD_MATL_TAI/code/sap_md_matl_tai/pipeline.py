from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_tai.config.ConfigStore import *
from sap_md_matl_tai.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_T023T = T023T(spark)
    df_T023T = collectMetrics(
        spark, 
        df_T023T, 
        "graph", 
        "cmMrkt_otIMo6u6XtvaWf$$aJt-3I7isB6I1132iDkA2", 
        "CLinUhPe8GEJtEtH5xVnK$$nOaqh0_wjfjuhkK0t4qGl"
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
    df_T134T = T134T(spark)
    df_T134T = collectMetrics(
        spark, 
        df_T134T, 
        "graph", 
        "_RC5pYNatq-hyS0WF6jtO$$Cg24z6xIR4S3Xd51A-uNT", 
        "vMDdYXHK41os-e0TzJXcE$$KB-FKlS7gIFk54xQ3Hni_"
    )
    df_DEL_MANDT_5 = DEL_MANDT_5(spark, df_T134T)
    MTBEZ_LU(spark, df_DEL_MANDT_5)
    df_MARA = MARA(spark)
    df_MARA = collectMetrics(
        spark, 
        df_MARA, 
        "graph", 
        "VXaooiFx1YqIH2gPaTDLu$$yCdZtaFAL3ikxR_k9PXgF", 
        "DYvpU9NaGq3GdNgziym5f$$igrVzU5G5QdasgxgItQX0"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_TAI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_TAI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
