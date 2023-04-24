from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.config.ConfigStore import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_06_MARA = DS_SAP_06_MARA(spark)
    df_MANDT_6 = MANDT_6(spark, df_DS_SAP_06_MARA)
    Lookup_1_MARA(spark, df_MANDT_6)
    df_DS_SAP_02_MCHB = DS_SAP_02_MCHB(spark)
    df_DS_SAP_03_MSKU = DS_SAP_03_MSKU(spark)
    df_DS_SAP_05_MSSL = DS_SAP_05_MSSL(spark)
    df_DS_SAP_04_MSLB = DS_SAP_04_MSLB(spark)
    df_MANDT_4 = MANDT_4(spark, df_DS_SAP_04_MSLB)
    df_DS_SAP_01_MARD = DS_SAP_01_MARD(spark)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_INV_BBA_BBL_BBN_GEU_MRS_MBP")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_INV_BBA_BBL_BBN_GEU_MRS_MBP")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
