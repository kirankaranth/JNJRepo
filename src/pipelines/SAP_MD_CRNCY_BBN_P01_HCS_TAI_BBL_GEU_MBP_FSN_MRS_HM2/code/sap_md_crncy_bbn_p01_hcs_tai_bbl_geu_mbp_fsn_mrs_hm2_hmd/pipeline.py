from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.config.ConfigStore import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_TCURC = DS_SAP_01_TCURC(spark)
    df_DS_SAP_01_TCURC = collectMetrics(
        spark, 
        df_DS_SAP_01_TCURC, 
        "graph", 
        "m-k9QBhfen2UT-0AvKGT_$$fuawXv4gmuyepbfPb5KFK", 
        "IfgytiNr6bbRbcknrwpOV$$xMSdxLNNMS2WbJCsVRn8-"
    )
    df_MANDT_FILTER_TCURC = MANDT_FILTER_TCURC(spark, df_DS_SAP_01_TCURC)
    df_SET_FIELD_TCURC = SET_FIELD_TCURC(spark, df_MANDT_FILTER_TCURC)
    df_DS_SAP_01_TCURX = DS_SAP_01_TCURX(spark)
    df_DS_SAP_01_TCURX = collectMetrics(
        spark, 
        df_DS_SAP_01_TCURX, 
        "graph", 
        "lUPNle1a7y-UZAXP4saF3$$tnYwVuAJT7nFe5Zw4rMPy", 
        "jGJv-lh9wp8A2cV4rQ_7d$$If40xU6XOqoGv_9ZXNaH3"
    )
    df_MANDT_FILTER_TCURX = MANDT_FILTER_TCURX(spark, df_DS_SAP_01_TCURX)
    df_SET_FIELD_TCURX = SET_FIELD_TCURX(spark, df_MANDT_FILTER_TCURX)
    df_Join_1 = Join_1(spark, df_SET_FIELD_TCURC, df_SET_FIELD_TCURX)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "7sUMeUniBmQTSLAl8cnNx$$tp00U9Igj0GoIt5apy0-c", 
        "d2LtJDJ-xcz2AJ_Cxf-4f$$wtCMIlc2t6v-y07PXJ_52"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "iqQ449NgrHREBvfCAXNO5$$Nb9d1v-_wOeBUmJDGxNub", 
        "-24zBXDCcL-9vEIPyaDEG$$Fc5mgYsp60S49wzShVZ84"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
    MD_CRNCY(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CRNCY_BBN_P01_HCS_TAI_BBL_GEU_MBP_FSN_MRS_HM2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CRNCY_BBN_P01_HCS_TAI_BBL_GEU_MBP_FSN_MRS_HM2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
