from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_line_hm2.config.ConfigStore import *
from sap_md_delv_line_hm2.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_line_hm2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_LIPS = DS_SAP_01_LIPS(spark)
    df_MANDT_FILTER_LIPS = MANDT_FILTER_LIPS(spark, df_DS_SAP_01_LIPS)
    df_DS_SAP_01_LIKP = DS_SAP_01_LIKP(spark)
    df_MANDT_FILTER_LIKP = MANDT_FILTER_LIKP(spark, df_DS_SAP_01_LIKP)
    df_DS_SAP_01_VBAK = DS_SAP_01_VBAK(spark)
    df_MANDT_FILTER_VBAK = MANDT_FILTER_VBAK(spark, df_DS_SAP_01_VBAK)
    df_DS_SAP_01_VBAP = DS_SAP_01_VBAP(spark)
    df_MANDT_FILTER_VBAP = MANDT_FILTER_VBAP(spark, df_DS_SAP_01_VBAP)
    df_DS_SAP_01_TVM4T = DS_SAP_01_TVM4T(spark)
    df_MANDT_FILTER_TVM4T = MANDT_FILTER_TVM4T(spark, df_DS_SAP_01_TVM4T)
    df_Join_1 = Join_1(
        spark, 
        df_MANDT_FILTER_LIPS, 
        df_MANDT_FILTER_LIKP, 
        df_MANDT_FILTER_VBAK, 
        df_MANDT_FILTER_VBAP, 
        df_MANDT_FILTER_TVM4T
    )
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    MD_DELV_LINE(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_DELV_LINE_HM2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_DELV_LINE_HM2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
