from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_line_bba_bbl_bbn.config.ConfigStore import *
from sap_md_delv_line_bba_bbl_bbn.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_line_bba_bbl_bbn.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_02_LIPS = DS_SAP_02_LIPS(spark)
    df_MANDT_FILTER_LIPS = MANDT_FILTER_LIPS(spark, df_DS_SAP_02_LIPS)
    df_DS_SAP_02_LIKP = DS_SAP_02_LIKP(spark)
    df_MANDT_FILTER_LIKP = MANDT_FILTER_LIKP(spark, df_DS_SAP_02_LIKP)
    df_DS_SAP_02_VBAK = DS_SAP_02_VBAK(spark)
    df_MANDT_FILTER_VBAK = MANDT_FILTER_VBAK(spark, df_DS_SAP_02_VBAK)
    df_DS_SAP_01_VBUP = DS_SAP_01_VBUP(spark)
    df_MANDT_FILTER_VBUP = MANDT_FILTER_VBUP(spark, df_DS_SAP_01_VBUP)
    df_Join_1 = Join_1(spark, df_MANDT_FILTER_LIPS, df_MANDT_FILTER_LIKP, df_MANDT_FILTER_VBAK, df_MANDT_FILTER_VBUP)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_DELV_LINE_BBA_BBL_BBN")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_DELV_LINE_BBA_BBL_BBN")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
