from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai.udfs.UDFs import *
from prophecy.utils import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_MARA_01 = DS_SAP_MARA_01(spark)
    df_MANDT1 = MANDT1(spark, df_DS_SAP_MARA_01)
    MEINS_LU(spark, df_MANDT1)
    df_DS_SAP_01_MARM = DS_SAP_01_MARM(spark)
    df_MANDT = MANDT(spark, df_DS_SAP_01_MARM)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_MANDT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set(
        "prophecy.metadata.pipeline.uri",
        "pipelines/MD_MATL_ALT_UOM_ATL_BBA_BBL_BBN_BWI_FSN_GEU_HCS_MBP_MRS_P01_SVS_TAI"
    )
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/MD_MATL_ALT_UOM_ATL_BBA_BBL_BBN_BWI_FSN_GEU_HCS_MBP_MRS_P01_SVS_TAI"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
