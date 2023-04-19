from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.udfs.UDFs import *
from prophecy.utils import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_T134T = T134T(spark)
    df_DEL_MANDT4_1 = DEL_MANDT4_1(spark, df_T134T)
    T134T_LU(spark, df_DEL_MANDT4_1)
    df_AUSP = AUSP(spark)
    df_DEL_MANDT = DEL_MANDT(spark, df_AUSP)
    df_CABN = CABN(spark)
    df_DEL_MANDT2 = DEL_MANDT2(spark, df_CABN)
    df_INOB = INOB(spark)
    df_D_M_OBTAB_KLART = D_M_OBTAB_KLART(spark, df_INOB)
    df_INOB_DEDUP = INOB_DEDUP(spark, df_D_M_OBTAB_KLART)
    df_CHARACTERISTICS = CHARACTERISTICS(spark, df_DEL_MANDT, df_INOB_DEDUP, df_DEL_MANDT2)
    df_MAT_SPEC = MAT_SPEC(spark, df_CHARACTERISTICS)
    MAT_SPEC_LU(spark, df_MAT_SPEC)
    df_DS_SAP_MAKT_01 = DS_SAP_MAKT_01(spark)
    df_DEL_MANDT4 = DEL_MANDT4(spark, df_DS_SAP_MAKT_01)
    MAKTX_LU(spark, df_DEL_MANDT4)
    df_SPEC_VER = SPEC_VER(spark, df_CHARACTERISTICS)
    SPEC_VER_LU(spark, df_SPEC_VER)
    df_DS_SAP_MARA_BBA_BBN = DS_SAP_MARA_BBA_BBN(spark)
    df_DEL_MANDT_1 = DEL_MANDT_1(spark, df_DS_SAP_MARA_BBA_BBN)
    df_DEL_MANDT3 = DEL_MANDT3(spark, df_DEL_MANDT_1)
    df_XFORM = XFORM(spark, df_DEL_MANDT3)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    TARGET(spark, df_SELECT_FIELDS)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_BBL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_BBL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
