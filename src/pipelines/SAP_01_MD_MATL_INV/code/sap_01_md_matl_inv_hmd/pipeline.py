from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_inv_hmd.config.ConfigStore import *
from sap_01_md_matl_inv_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_inv_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_04_MARA = DS_SAP_04_MARA(spark)
    df_DS_SAP_04_MARA = collectMetrics(
        spark, 
        df_DS_SAP_04_MARA, 
        "graph", 
        "SV_z9iSmIqB_QJ3AYSfQf$$-nMg3t73mmzf1Te8SaTlS", 
        "OWoUsNv4KeNM4ENPAgtYl$$q7zP3HNnFaQch2-jEfiQM"
    )
    df_MANDT_4 = MANDT_4(spark, df_DS_SAP_04_MARA)
    Lookup_1_MARA(spark, df_MANDT_4)
    df_DS_SAP_01_NSDM_V_MARD = DS_SAP_01_NSDM_V_MARD(spark)
    df_DS_SAP_01_NSDM_V_MARD = collectMetrics(
        spark, 
        df_DS_SAP_01_NSDM_V_MARD, 
        "graph", 
        "wb9ccLNGAUgX-ae0KAPPk$$VaAEwhYmO_vAAJA3dw9u-", 
        "D_qFnYIYQV4HHNOjpY3xf$$8uKRp-2H0Pu6stMnBc0Kl"
    )
    df_MANDT_1 = MANDT_1(spark, df_DS_SAP_01_NSDM_V_MARD)
    df_Select_MARD_Columns = Select_MARD_Columns(spark, df_MANDT_1)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark, df_Select_MARD_Columns)
    df_DS_SAP_03_NSDM_V_MSKU = DS_SAP_03_NSDM_V_MSKU(spark)
    df_DS_SAP_03_NSDM_V_MSKU = collectMetrics(
        spark, 
        df_DS_SAP_03_NSDM_V_MSKU, 
        "graph", 
        "CU_T734zOOMvYKkMETFQp$$jJTDA57UqWo8G7koGqqUQ", 
        "8jBhB-eaHu_5pfv2H56rb$$vVRL9Kcm_Lz1jJ4m-_zM6"
    )
    df_MANDT_3 = MANDT_3(spark, df_DS_SAP_03_NSDM_V_MSKU)
    df_SchemaTransform_3_MSKU = SchemaTransform_3_MSKU(spark, df_MANDT_3)
    df_DS_SAP_02_NSDM_V_MCHB = DS_SAP_02_NSDM_V_MCHB(spark)
    df_DS_SAP_02_NSDM_V_MCHB = collectMetrics(
        spark, 
        df_DS_SAP_02_NSDM_V_MCHB, 
        "graph", 
        "q3vGQO3pQ1Bj1nDs-4bpS$$t9P4ox7-NiJ146VahL8d3", 
        "6y3kn5lgb8q9hITZ6JNMy$$yv7Qqy2tZCshfxarf3C9j"
    )
    df_MANDT_2 = MANDT_2(spark, df_DS_SAP_02_NSDM_V_MCHB)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark, df_MANDT_2)
    df_SET_FIELD_ORDER_1 = SET_FIELD_ORDER_1(spark, df_SchemaTransform_2_MCHB)
    df_SET_FIELD_ORDER_3 = SET_FIELD_ORDER_3(spark, df_SchemaTransform_3_MSKU)
    df_SET_FIELD_ORDER_1_1 = SET_FIELD_ORDER_1_1(spark, df_SchemaTransform_1_MARD)
    df_SetOperation_1_Union = SetOperation_1_Union(
        spark, 
        df_SET_FIELD_ORDER_1_1, 
        df_SET_FIELD_ORDER_1, 
        df_SET_FIELD_ORDER_3
    )
    df_SetOperation_1_Union = collectMetrics(
        spark, 
        df_SetOperation_1_Union, 
        "graph", 
        "EMPOgHtQpuRsGWNcmpDxS$$qCRww8Ke768D7sc03dzwW", 
        "yi_HwsOd6GAtGAP1Ylsxy$$aX3L3Si7GT9qfbF4-yQGe"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_MATL_INV")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_MATL_INV")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
