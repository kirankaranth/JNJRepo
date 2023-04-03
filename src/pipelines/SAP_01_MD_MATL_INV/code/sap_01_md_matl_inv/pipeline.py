from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_inv.config.ConfigStore import *
from sap_01_md_matl_inv.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_inv.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_MARA = DS_SAP_01_MARA(spark)
    Lookup_1_MARA(spark, df_DS_SAP_01_MARA)
    df_DS_SAP_01_NSDM_V_MARD = DS_SAP_01_NSDM_V_MARD(spark)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark, df_DS_SAP_01_NSDM_V_MARD)
    df_DS_SAP_01_NSDM_V_MSKU = DS_SAP_01_NSDM_V_MSKU(spark)
    df_SchemaTransform_3_MSKU = SchemaTransform_3_MSKU(spark, df_DS_SAP_01_NSDM_V_MSKU)
    df_SET_FIELD_ORDER = SET_FIELD_ORDER(spark, df_SchemaTransform_1_MARD)
    df_DS_SAP_01_NSDM_V_MCHB = DS_SAP_01_NSDM_V_MCHB(spark)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark, df_DS_SAP_01_NSDM_V_MCHB)
    df_SetOperation_1_Union = SetOperation_1_Union(spark, df_SchemaTransform_2_MCHB, df_SchemaTransform_3_MSKU)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_MATL_INV")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_MATL_INV")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
