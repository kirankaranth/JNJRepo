from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_inv_mbp.config.ConfigStore import *
from sap_md_matl_inv_mbp.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_inv_mbp.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_06_MARA = DS_SAP_06_MARA(spark)
    df_DS_SAP_06_MARA = collectMetrics(
        spark, 
        df_DS_SAP_06_MARA, 
        "graph", 
        "sgOdu0rwWrCp-dspSTEKr$$WbV6Vi5BBrNyLeiFbjJJ9", 
        "uYsXrXc8oA-bMrSrRl6v4$$ijzptWqnkWqtTczSRcpB5"
    )
    df_MANDT_6 = MANDT_6(spark, df_DS_SAP_06_MARA)
    Lookup_1_MARA(spark, df_MANDT_6)
    df_DS_SAP_02_MCHB = DS_SAP_02_MCHB(spark)
    df_DS_SAP_02_MCHB = collectMetrics(
        spark, 
        df_DS_SAP_02_MCHB, 
        "graph", 
        "1YKeBqNZH7LIuzxHjp9f4$$S65o6_pMG3W2OjRYrDd9s", 
        "a3c4fqA0dJ5GH0iqcdnMT$$vF08UI9hvWSIRQROMxXEt"
    )
    df_DS_SAP_03_MSKU = DS_SAP_03_MSKU(spark)
    df_DS_SAP_03_MSKU = collectMetrics(
        spark, 
        df_DS_SAP_03_MSKU, 
        "graph", 
        "BpF7w11fwNSFP4NEJM8Zn$$FA93DzRtpOoORpo76KkwA", 
        "F-1kv7W8KMiVygABebLYN$$uCDPzJocQzx6emhiM_2XF"
    )
    df_DS_SAP_05_MSSL = DS_SAP_05_MSSL(spark)
    df_DS_SAP_05_MSSL = collectMetrics(
        spark, 
        df_DS_SAP_05_MSSL, 
        "graph", 
        "-ugKuASLgK-hS-hlMOR-8$$jfymCyKVUIxH2yJmoBUvK", 
        "Hn_vnmO1bPRKmKglqn1Bw$$uMBa6C1t03VsCRqFkIXyo"
    )
    df_DS_SAP_04_MSLB = DS_SAP_04_MSLB(spark)
    df_DS_SAP_04_MSLB = collectMetrics(
        spark, 
        df_DS_SAP_04_MSLB, 
        "graph", 
        "BJ2US32OBOImTYXda_Ns2$$HjJ0fmK9If7Lpv9wOig-p", 
        "1aRW56Ssp2kr_ydusfzFJ$$5eh8tOUvzam7bVuYL02pR"
    )
    df_MANDT_4 = MANDT_4(spark, df_DS_SAP_04_MSLB)
    df_DS_SAP_01_MARD = DS_SAP_01_MARD(spark)
    df_DS_SAP_01_MARD = collectMetrics(
        spark, 
        df_DS_SAP_01_MARD, 
        "graph", 
        "v3iQSr3X0y28FIeXtXF0z$$RE7gI3cSL1QbOT8RRRBfA", 
        "O6PaMEs_KRNQQOoq38tCt$$sJhnklnoJ0csmH_spMtcL"
    )
    df_MANDT_5 = MANDT_5(spark, df_DS_SAP_05_MSSL)
    df_SchemaTransform_5_MSSL = SchemaTransform_5_MSSL(spark, df_MANDT_5)
    df_SET_FIELD_ORDER_5 = SET_FIELD_ORDER_5(spark, df_SchemaTransform_5_MSSL)
    df_SchemaTransform_4_MSLB = SchemaTransform_4_MSLB(spark, df_MANDT_4)
    df_SET_FIELD_ORDER_4 = SET_FIELD_ORDER_4(spark, df_SchemaTransform_4_MSLB)
    df_MANDT_1 = MANDT_1(spark, df_DS_SAP_01_MARD)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark, df_MANDT_1)
    df_SET_FIELD_ORDER_1 = SET_FIELD_ORDER_1(spark, df_SchemaTransform_1_MARD)
    df_MANDT_2 = MANDT_2(spark, df_DS_SAP_02_MCHB)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark, df_MANDT_2)
    df_SET_FIELD_ORDER_2 = SET_FIELD_ORDER_2(spark, df_SchemaTransform_2_MCHB)
    df_MANDT_3 = MANDT_3(spark, df_DS_SAP_03_MSKU)
    df_SchemaTransform_3_MSKU = SchemaTransform_3_MSKU(spark, df_MANDT_3)
    df_SET_FIELD_ORDER_3 = SET_FIELD_ORDER_3(spark, df_SchemaTransform_3_MSKU)
    df_SetOperation_1_Union = SetOperation_1_Union(
        spark, 
        df_SET_FIELD_ORDER_1, 
        df_SET_FIELD_ORDER_2, 
        df_SET_FIELD_ORDER_3, 
        df_SET_FIELD_ORDER_4, 
        df_SET_FIELD_ORDER_5
    )
    df_SetOperation_1_Union = collectMetrics(
        spark, 
        df_SetOperation_1_Union, 
        "graph", 
        "XjZeMWY0El38UbwVN6Obr$$uouHNsXVBrJC4eTIuvY1f", 
        "Zti2Tw3sNIz_I_4u6jSq2$$6POBBOWCyCI9zuMwhdgZw"
    )
    MD_MATL_INV(spark, df_SetOperation_1_Union)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_INV_MBP")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_INV_MBP")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
