from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_inv_hcs.config.ConfigStore import *
from sap_md_matl_inv_hcs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_inv_hcs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_0004_MARA = DS_SAP_0004_MARA(spark)
    df_DS_SAP_0004_MARA = collectMetrics(
        spark, 
        df_DS_SAP_0004_MARA, 
        "graph", 
        "McwxRe_NFN7f4GyhQjFlL$$4MLRzhOMbYN8y1T3Mb4W8", 
        "xAy0LCYJVQ7sX47AmC0Zr$$v93XErYDcpTZolFVAYiWU"
    )
    df_MANDT_4 = MANDT_4(spark, df_DS_SAP_0004_MARA)
    df_SELECT_MARA = SELECT_MARA(spark, df_MANDT_4)
    Lookup_1_MARA(spark, df_SELECT_MARA)
    df_DS_SAP_0002_MCHB = DS_SAP_0002_MCHB(spark)
    df_DS_SAP_0002_MCHB = collectMetrics(
        spark, 
        df_DS_SAP_0002_MCHB, 
        "graph", 
        "KOwyqgszu8WWk7JcddBa_$$QamWNrWgxAl9BMEbCSx7r", 
        "Ypyt8HuunR29tLTrRGrzo$$1lduUmJiu816Fl2gsblMz"
    )
    df_MANDT_2 = MANDT_2(spark, df_DS_SAP_0002_MCHB)
    df_Select_MCHB = Select_MCHB(spark, df_MANDT_2)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark, df_Select_MCHB)
    df_DS_SAP_0003_MSKU = DS_SAP_0003_MSKU(spark)
    df_DS_SAP_0003_MSKU = collectMetrics(
        spark, 
        df_DS_SAP_0003_MSKU, 
        "graph", 
        "QMPtrnXDX1tk6O6gq80ke$$iDie3GxcU9n-py9_eKbso", 
        "Yi-ibPOH3YlFvtMXvJEIn$$3yAMBbW_siLQPu8vntC13"
    )
    df_MANDT_3 = MANDT_3(spark, df_DS_SAP_0003_MSKU)
    df_SELECT_MSKU = SELECT_MSKU(spark, df_MANDT_3)
    df_SchemaTransform_3_MSKU = SchemaTransform_3_MSKU(spark, df_SELECT_MSKU)
    df_SET_FIELD_ORDER_3 = SET_FIELD_ORDER_3(spark, df_SchemaTransform_3_MSKU)
    df_DS_SAP_0001_MARD = DS_SAP_0001_MARD(spark)
    df_DS_SAP_0001_MARD = collectMetrics(
        spark, 
        df_DS_SAP_0001_MARD, 
        "graph", 
        "I1oXhTJ0-A0QX6Ugzjki7$$iBeGNVAWlq1ehPSU33LWm", 
        "qhk613LfIhVL7qGJ2t7w2$$BY6-4gRN_DifAaZjo-3OH"
    )
    df_MANDT_1 = MANDT_1(spark, df_DS_SAP_0001_MARD)
    df_Select_MARD_Columns = Select_MARD_Columns(spark, df_MANDT_1)
    df_SET_FIELD_ORDER_2 = SET_FIELD_ORDER_2(spark, df_SchemaTransform_2_MCHB)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark, df_Select_MARD_Columns)
    df_SET_FIELD_ORDER_1 = SET_FIELD_ORDER_1(spark, df_SchemaTransform_1_MARD)
    df_SetOperation_1_Union = SetOperation_1_Union(
        spark, 
        df_SET_FIELD_ORDER_1, 
        df_SET_FIELD_ORDER_2, 
        df_SET_FIELD_ORDER_3
    )
    df_SetOperation_1_Union = collectMetrics(
        spark, 
        df_SetOperation_1_Union, 
        "graph", 
        "3WAQejLZoWKhw2sduLyS3$$utMJ1aKxsVWbViZhmcrnF", 
        "EnEDhnL6KJQUDyU3rPtwJ$$DhLlBf5VWK--blSMxkwsk"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_INV_HCS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_INV_HCS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
