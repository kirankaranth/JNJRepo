from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_inv_p01.config.ConfigStore import *
from sap_md_matl_inv_p01.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_inv_p01.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_MARA = DS_SAP_01_MARA(spark)
    df_MANDT_4 = MANDT_4(spark, df_DS_SAP_01_MARA)
    Lookup_1_MARA(spark, df_MANDT_4)
    df_DS_SAP_01_NSDM_V_MSKU = DS_SAP_01_NSDM_V_MSKU(spark)
    df_MANDT_3 = MANDT_3(spark, df_DS_SAP_01_NSDM_V_MSKU)
    df_SchemaTransform_3_MSKU = SchemaTransform_3_MSKU(spark, df_MANDT_3)
    df_SET_FIELD_ORDER_3 = SET_FIELD_ORDER_3(spark, df_SchemaTransform_3_MSKU)
    df_DS_SAP_01_NSDM_V_MCHB = DS_SAP_01_NSDM_V_MCHB(spark)
    df_DS_SAP_01_NSDM_V_MARD = DS_SAP_01_NSDM_V_MARD(spark)
    df_MANDT_1 = MANDT_1(spark, df_DS_SAP_01_NSDM_V_MARD)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark, df_MANDT_1)
    df_SET_FIELD_ORDER = SET_FIELD_ORDER(spark, df_SchemaTransform_1_MARD)
    df_MANDT_2 = MANDT_2(spark, df_DS_SAP_01_NSDM_V_MCHB)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark, df_MANDT_2)
    df_SET_FIELD_ORDER_1 = SET_FIELD_ORDER_1(spark, df_SchemaTransform_2_MCHB)
    df_SetOperation_1_Union = SetOperation_1_Union(
        spark, 
        df_SET_FIELD_ORDER, 
        df_SET_FIELD_ORDER_1, 
        df_SET_FIELD_ORDER_3
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_INV_P01")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_INV_P01")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
