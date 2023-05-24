from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_valut_hist.config.ConfigStore import *
from sap_md_matl_valut_hist.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_valut_hist.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_MBEWH = DS_SAP_01_MBEWH(spark)
    df_DS_SAP_02_MARA = DS_SAP_02_MARA(spark)
    df_MANDT_FILTER_MARA = MANDT_FILTER_MARA(spark, df_DS_SAP_02_MARA)
    df_Reformat_MARA = Reformat_MARA(spark, df_MANDT_FILTER_MARA)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_01_MBEWH)
    df_Join_1 = Join_1(spark, df_MANDT_FILTER, df_Reformat_MARA)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_ORDER_FIELD_REFORMAT = SET_ORDER_FIELD_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_ORDER_FIELD_REFORMAT)
    MD_MATL_VALUT_HIST(spark, df_SET_ORDER_FIELD_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_VALUT_HIST")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_VALUT_HIST")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
