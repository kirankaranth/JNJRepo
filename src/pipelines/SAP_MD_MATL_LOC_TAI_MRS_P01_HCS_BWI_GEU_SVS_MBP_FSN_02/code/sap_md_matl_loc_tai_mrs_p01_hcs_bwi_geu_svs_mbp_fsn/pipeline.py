from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_loc_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn.config.ConfigStore import *
from sap_md_matl_loc_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_loc_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_MARC = SAP_MARC(spark)
    df_SAP_MARC = collectMetrics(
        spark, 
        df_SAP_MARC, 
        "graph", 
        "J4_uS3Q4GhLzOZu0ywART$$NUBQymIdgcgIKQgmw18UQ", 
        "XKQAzwCyxKqPPiRqgbCB_$$oQrRD8nAiVmZB6zBLPCWx"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_MARC)
    df_NEW_FIELDS_PK = NEW_FIELDS_PK(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_PK)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "zqgwxFtlSFlDyzhEbnTk6$$d-94H6BgyeU15erzMnsDT", 
        "NnAGYtZnDQCWq1rBcnPym$$K9nn3cuIq6Mwyd4w4i5w2"
    )
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
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_LOC_TAI_MRS_P01_HCS_BWI_GEU_SVS_MBP_FSN_02")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_LOC_TAI_MRS_P01_HCS_BWI_GEU_SVS_MBP_FSN_02")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
