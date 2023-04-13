from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.udfs.UDFs import *
from prophecy.utils import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_MAKT = MAKT(spark)
    df_DEL_MANDT4 = DEL_MANDT4(spark, df_MAKT)
    MAKTX_LU(spark, df_DEL_MANDT4)
    df_MARA = MARA(spark)
    df_DEL_MANDT3 = DEL_MANDT3(spark, df_MARA)
    df_XFORM = XFORM(spark, df_DEL_MANDT3)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    df_CABN = CABN(spark)
    df_DEL_MANDT2 = DEL_MANDT2(spark, df_CABN)
    df_INOB = INOB(spark)
    df_AUSP = AUSP(spark)
    df_DEL_MANDT = DEL_MANDT(spark, df_AUSP)
    TARGET(spark, df_SELECT_FIELDS)
    df_D_M_OBTAB = D_M_OBTAB(spark, df_INOB)
    df_SQLStatement_1 = SQLStatement_1(spark, df_DEL_MANDT, df_D_M_OBTAB, df_DEL_MANDT2)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_MATL_SAP_ATL_BWI_FSN_GEU_MRS_P01_SVS_TAI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_MATL_SAP_ATL_BWI_FSN_GEU_MRS_P01_SVS_TAI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
