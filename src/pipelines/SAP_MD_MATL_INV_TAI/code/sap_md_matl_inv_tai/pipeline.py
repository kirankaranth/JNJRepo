from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_inv_tai.config.ConfigStore import *
from sap_md_matl_inv_tai.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_inv_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_000003_MARA = DS_SAP_000003_MARA(spark)
    df_MANDT_6 = MANDT_6(spark, df_DS_SAP_000003_MARA)
    Lookup_1_MARA(spark, df_MANDT_6)
    df_DS_SAP_000002_MCHB = DS_SAP_000002_MCHB(spark)
    df_MANDT_2 = MANDT_2(spark, df_DS_SAP_000002_MCHB)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark, df_MANDT_2)
    df_SET_FIELD_ORDER_2 = SET_FIELD_ORDER_2(spark, df_SchemaTransform_2_MCHB)
    df_DS_SAP_000001_MARD = DS_SAP_000001_MARD(spark)
    df_MANDT_1 = MANDT_1(spark, df_DS_SAP_000001_MARD)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark, df_MANDT_1)
    df_SET_FIELD_ORDER_1 = SET_FIELD_ORDER_1(spark, df_SchemaTransform_1_MARD)
    df_SetOperation_1_Union = SetOperation_1_Union(spark, df_SET_FIELD_ORDER_1, df_SET_FIELD_ORDER_2)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_INV_TAI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_INV_TAI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
