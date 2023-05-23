from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.config.ConfigStore import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_02_TVM4T = DS_SAP_02_TVM4T(spark)
    df_DS_SAP_04_VBAK = DS_SAP_04_VBAK(spark)
    df_DS_SAP_04_LIPS = DS_SAP_04_LIPS(spark)
    df_MANDT_FILTER_LIPS = MANDT_FILTER_LIPS(spark, df_DS_SAP_04_LIPS)
    df_SELECT_LIPS = SELECT_LIPS(spark, df_MANDT_FILTER_LIPS)
    df_DS_SAP_04_LIKP = DS_SAP_04_LIKP(spark)
    df_MANDT_FILTER_LIKP = MANDT_FILTER_LIKP(spark, df_DS_SAP_04_LIKP)
    df_SELECT_LIKP = SELECT_LIKP(spark, df_MANDT_FILTER_LIKP)
    df_MANDT_FILTER_VBAK = MANDT_FILTER_VBAK(spark, df_DS_SAP_04_VBAK)
    df_SELECT_VBAK = SELECT_VBAK(spark, df_MANDT_FILTER_VBAK)
    df_DS_SAP_03_VBUP = DS_SAP_03_VBUP(spark)
    df_MANDT_FILTER_VBUP = MANDT_FILTER_VBUP(spark, df_DS_SAP_03_VBUP)
    df_SELECT_VBUP = SELECT_VBUP(spark, df_MANDT_FILTER_VBUP)
    df_MANDT_FILTER_TVM4T = MANDT_FILTER_TVM4T(spark, df_DS_SAP_02_TVM4T)
    df_Reformat_1 = Reformat_1(spark, df_MANDT_FILTER_TVM4T)
    df_Join_1 = Join_1(spark, df_SELECT_LIPS, df_SELECT_LIKP, df_SELECT_VBAK, df_SELECT_VBUP, df_Reformat_1)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    MD_DELV_LINE(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_DELV_LINE_FSN_GEU_HCS_MRS_P01_SVS_ATL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_DELV_LINE_FSN_GEU_HCS_MRS_P01_SVS_ATL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
