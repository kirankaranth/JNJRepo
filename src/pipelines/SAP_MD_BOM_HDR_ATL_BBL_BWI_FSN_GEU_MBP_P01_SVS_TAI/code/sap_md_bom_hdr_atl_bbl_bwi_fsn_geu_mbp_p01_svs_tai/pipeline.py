from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_bom_hdr_atl_bbl_bwi_fsn_geu_mbp_p01_svs_tai.config.ConfigStore import *
from sap_md_bom_hdr_atl_bbl_bwi_fsn_geu_mbp_p01_svs_tai.udfs.UDFs import *
from prophecy.utils import *
from sap_md_bom_hdr_atl_bbl_bwi_fsn_geu_mbp_p01_svs_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_02_STKO = SAP_02_STKO(spark)
    df_STKO_MANDT_FILTER = STKO_MANDT_FILTER(spark, df_SAP_02_STKO)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_STKO_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_PK_COUNT = PK_COUNT(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_PK_GT_1 = PK_GT_1(spark, df_PK_COUNT)
    MD_BOM_HDR(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_BOM_HDR_ATL_BBL_BWI_FSN_GEU_MBP_P01_SVS_TAI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_BOM_HDR_ATL_BBL_BWI_FSN_GEU_MBP_P01_SVS_TAI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
