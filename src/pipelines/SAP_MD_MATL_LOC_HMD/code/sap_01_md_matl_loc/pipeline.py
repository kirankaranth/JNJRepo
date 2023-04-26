from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_loc.graph import *

def pipeline(spark: SparkSession) -> None:
    df_T141T = T141T(spark)
    df_T024 = T024(spark)
    df_T024D = T024D(spark)
    df_MANDT_FILTER_2 = MANDT_FILTER_2(spark, df_T024D)
    df_NSDM_V_MARC = NSDM_V_MARC(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_NSDM_V_MARC)
    df_MANDT_FILTER_1 = MANDT_FILTER_1(spark, df_T141T)
    df_Join_1 = Join_1(spark, df_MANDT_FILTER, df_MANDT_FILTER_1)
    df_Join_2 = Join_2(spark, df_Join_1, df_MANDT_FILTER_2)
    df_T024F = T024F(spark)
    df_MANDT_FILTER_3 = MANDT_FILTER_3(spark, df_T024F)
    df_Join_3 = Join_3(spark, df_Join_2, df_MANDT_FILTER_3)
    df_MANDT_FILTER_4 = MANDT_FILTER_4(spark, df_T024)
    df_Join_4 = Join_4(spark, df_Join_3, df_MANDT_FILTER_4)
    df_NEW_FIELDS_PK = NEW_FIELDS_PK(spark, df_Join_4)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_PK)
    MD_MATL_LOC(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_LOC_HMD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_LOC_HMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
