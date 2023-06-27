from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_inv_p01_svs.config.ConfigStore import *
from sap_md_matl_inv_p01_svs.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_inv_p01_svs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_07_MARA = DS_SAP_07_MARA(spark)
    df_DS_SAP_07_MARA = collectMetrics(
        spark, 
        df_DS_SAP_07_MARA, 
        "graph", 
        "mzJWZmLoZglzwax-5meNV$$mQO_1NAg4dbCXlvE4yVbk", 
        "S_VgeAEEYctJbdU0l_lrC$$8JgOagKyRDcEAFFOw1M9L"
    )
    df_MANDT_7 = MANDT_7(spark, df_DS_SAP_07_MARA)
    df_SELECT_MARA = SELECT_MARA(spark, df_MANDT_7)
    Lookup_1_MARA(spark, df_SELECT_MARA)
    df_DS_SAP_02_MCHB = DS_SAP_02_MCHB(spark)
    df_DS_SAP_02_MCHB = collectMetrics(
        spark, 
        df_DS_SAP_02_MCHB, 
        "graph", 
        "2EL3rj3RuTS9vQy_hpAhX$$OKAv63zubzlpQ2bOQU342", 
        "w7VRydRxijVOVD64Af5VQ$$uDjUUJrnvMTEF_ufU6Dzl"
    )
    df_DS_SAP_03_MSKU = DS_SAP_03_MSKU(spark)
    df_DS_SAP_03_MSKU = collectMetrics(
        spark, 
        df_DS_SAP_03_MSKU, 
        "graph", 
        "9onsA3JKbOKD0ICxCirGP$$nzZ-txLya-vFy6dtIR1sA", 
        "bbNfjZdqSr7XixIcr1H5-$$-uH_j0gyLa0fq2jE_EmIs"
    )
    df_MANDT_3 = MANDT_3(spark, df_DS_SAP_03_MSKU)
    df_SELECT_MSKU = SELECT_MSKU(spark, df_MANDT_3)
    df_SchemaTransform_3_MSKU = SchemaTransform_3_MSKU(spark, df_SELECT_MSKU)
    df_SET_FIELD_ORDER_3 = SET_FIELD_ORDER_3(spark, df_SchemaTransform_3_MSKU)
    df_DS_SAP_04_MKOL = DS_SAP_04_MKOL(spark)
    df_DS_SAP_04_MKOL = collectMetrics(
        spark, 
        df_DS_SAP_04_MKOL, 
        "graph", 
        "p33quiu9TaPo8J1QDarOy$$0Q5RtUWpfDtaYhwajEpge", 
        "F1sei3nTNGGi0ZC51F5UK$$h33x1XpzjSguUsQDUKBh2"
    )
    df_DS_SAP_01_MARD = DS_SAP_01_MARD(spark)
    df_DS_SAP_01_MARD = collectMetrics(
        spark, 
        df_DS_SAP_01_MARD, 
        "graph", 
        "vwP8ZICtDI7T03mzekNMl$$7j6njoEr-K5ZBjsuEfnxl", 
        "gusmkmhzxfg-m3Ez87_U-$$xr0bHVCyDaGmK0rc0pI6J"
    )
    df_MANDT_1 = MANDT_1(spark, df_DS_SAP_01_MARD)
    df_Select_MARD_Columns = Select_MARD_Columns(spark, df_MANDT_1)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark, df_Select_MARD_Columns)
    df_SET_FIELD_ORDER_1 = SET_FIELD_ORDER_1(spark, df_SchemaTransform_1_MARD)
    df_MANDT_2 = MANDT_2(spark, df_DS_SAP_02_MCHB)
    df_Select_MCHB = Select_MCHB(spark, df_MANDT_2)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark, df_Select_MCHB)
    df_SET_FIELD_ORDER_2 = SET_FIELD_ORDER_2(spark, df_SchemaTransform_2_MCHB)
    df_MANDT_4 = MANDT_4(spark, df_DS_SAP_04_MKOL)
    df_Select_MKOL = Select_MKOL(spark, df_MANDT_4)
    df_SchemaTransform_4_MKOL = SchemaTransform_4_MKOL(spark, df_Select_MKOL)
    df_SET_FIELD_ORDER_4 = SET_FIELD_ORDER_4(spark, df_SchemaTransform_4_MKOL)
    df_DS_SAP_05_MSLB = DS_SAP_05_MSLB(spark)
    df_DS_SAP_05_MSLB = collectMetrics(
        spark, 
        df_DS_SAP_05_MSLB, 
        "graph", 
        "Ivu3fxWMsxiCMhtr6Nuch$$p0VqhGWWbBjB-uCSeFo_w", 
        "6t1adWWr6uxTTLEb1Ujb2$$-7sXl3s4Ts_UFn9GNirNp"
    )
    df_MANDT_5 = MANDT_5(spark, df_DS_SAP_05_MSLB)
    df_SELECT_MSLB = SELECT_MSLB(spark, df_MANDT_5)
    df_SchemaTransform_5_MSLB = SchemaTransform_5_MSLB(spark, df_SELECT_MSLB)
    df_SET_FIELD_ORDER_5 = SET_FIELD_ORDER_5(spark, df_SchemaTransform_5_MSLB)
    df_DS_SAP_06_MSSL = DS_SAP_06_MSSL(spark)
    df_DS_SAP_06_MSSL = collectMetrics(
        spark, 
        df_DS_SAP_06_MSSL, 
        "graph", 
        "vmWoaZWpc4D96sHp1u1if$$XGtucJEo_ocdKEpWhgfBT", 
        "gokTkjmynwCKuzAWRWI_g$$tf15VFjatjB_1B4Ex4JNq"
    )
    df_MANDT_6 = MANDT_6(spark, df_DS_SAP_06_MSSL)
    df_SELECT_MSSL = SELECT_MSSL(spark, df_MANDT_6)
    df_SchemaTransform_6_MSSL = SchemaTransform_6_MSSL(spark, df_SELECT_MSSL)
    df_SET_FIELD_ORDER_6 = SET_FIELD_ORDER_6(spark, df_SchemaTransform_6_MSSL)
    df_SetOperation_1_Union = SetOperation_1_Union(
        spark, 
        df_SET_FIELD_ORDER_1, 
        df_SET_FIELD_ORDER_2, 
        df_SET_FIELD_ORDER_3, 
        df_SET_FIELD_ORDER_4, 
        df_SET_FIELD_ORDER_5, 
        df_SET_FIELD_ORDER_6
    )
    df_SetOperation_1_Union = collectMetrics(
        spark, 
        df_SetOperation_1_Union, 
        "graph", 
        "2bJtzJZJVPPcTX6PWTEeQ$$o6XwwehhEMGxx7QAjrA4E", 
        "tMidJBXvz9C9HIuQjQO48$$gZSWztyYUUcdEsQ81O8r1"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_INV_P01")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_INV_P01")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
