from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_loc.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_T024 = SAP_T024(spark)
    df_MANDT_FILTER_05 = MANDT_FILTER_05(spark, df_SAP_T024)
    df_SAP_NSDM_V_MARC = SAP_NSDM_V_MARC(spark)
    df_MANDT_FILTER_01 = MANDT_FILTER_01(spark, df_SAP_NSDM_V_MARC)
    df_SAP_T141T = SAP_T141T(spark)
    df_MANDT_FILTER_02 = MANDT_FILTER_02(spark, df_SAP_T141T)
    df_SAP_T024D = SAP_T024D(spark)
    df_MANDT_FILTER_03 = MANDT_FILTER_03(spark, df_SAP_T024D)
    df_SAP_T024F = SAP_T024F(spark)
    df_MANDT_FILTER_04 = MANDT_FILTER_04(spark, df_SAP_T024F)
    df_Join_1 = Join_1(
        spark, 
        df_MANDT_FILTER_01, 
        df_MANDT_FILTER_02, 
        df_MANDT_FILTER_03, 
        df_MANDT_FILTER_04, 
        df_MANDT_FILTER_05
    )
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELDS_ORDER_REFORMAT = SET_FIELDS_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    SAP_MD_MATL_LOC(spark, df_SET_FIELDS_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_MATL_LOC")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_MATL_LOC")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
