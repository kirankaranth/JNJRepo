from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.config.ConfigStore import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_TCURC = DS_SAP_01_TCURC(spark)
    df_DS_SAP_01_TCURX = DS_SAP_01_TCURX(spark)
    df_MANDT_FILTER_TCURX = MANDT_FILTER_TCURX(spark, df_DS_SAP_01_TCURX)
    df_MANDT_FILTER_TCURC = MANDT_FILTER_TCURC(spark, df_DS_SAP_01_TCURC)
    df_Join_1 = Join_1(spark, df_MANDT_FILTER_TCURC, df_MANDT_FILTER_TCURX)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CRNCY_BBN_P01_HCS_TAI_BBL_GEU_MBP_FSN_MRS_HM2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CRNCY_BBN_P01_HCS_TAI_BBL_GEU_MBP_FSN_MRS_HM2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
