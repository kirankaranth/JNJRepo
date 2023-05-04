from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_T001 = SAP_T001(spark)
    df_MANDT_FILTER_T001 = MANDT_FILTER_T001(spark, df_SAP_T001)
    LU_SAP_T001(spark, df_MANDT_FILTER_T001)
    df_SAP_TVV2T = SAP_TVV2T(spark)
    df_MANDT_FILTER_TVV2T = MANDT_FILTER_TVV2T(spark, df_SAP_TVV2T)
    LU_SAP_TVV2T(spark, df_MANDT_FILTER_TVV2T)
    df_SAP_TVTWT = SAP_TVTWT(spark)
    df_MANDT_FILTER_TVTWT = MANDT_FILTER_TVTWT(spark, df_SAP_TVTWT)
    LU_SAP_TVTWT(spark, df_MANDT_FILTER_TVTWT)
    df_SAP_TVFST = SAP_TVFST(spark)
    df_MANDT_FILTER_TVFST = MANDT_FILTER_TVFST(spark, df_SAP_TVFST)
    LU_SAP_TVFST(spark, df_MANDT_FILTER_TVFST)
    df_SAP_TVV3T = SAP_TVV3T(spark)
    df_MANDT_FILTER_TVV3T = MANDT_FILTER_TVV3T(spark, df_SAP_TVV3T)
    LU_SAP_TVV3T(spark, df_MANDT_FILTER_TVV3T)
    df_SAP_TVAKT = SAP_TVAKT(spark)
    df_MANDT_FILTER_TVAKT = MANDT_FILTER_TVAKT(spark, df_SAP_TVAKT)
    LU_SAP_TVAKT(spark, df_MANDT_FILTER_TVAKT)
    df_SAP_TVKOT = SAP_TVKOT(spark)
    df_MANDT_FILTER_TVKOT = MANDT_FILTER_TVKOT(spark, df_SAP_TVKOT)
    LU_SAP_TVKOT(spark, df_MANDT_FILTER_TVKOT)
    df_SAP_TVAU = SAP_TVAU(spark)
    df_MANDT_FILTER_TVAU = MANDT_FILTER_TVAU(spark, df_SAP_TVAU)
    LU_SAP_TVAU(spark, df_MANDT_FILTER_TVAU)
    df_SAP_TVV1T = SAP_TVV1T(spark)
    df_MANDT_FILTER_TVV1T = MANDT_FILTER_TVV1T(spark, df_SAP_TVV1T)
    LU_SAP_TVV1T(spark, df_MANDT_FILTER_TVV1T)
    df_SAP_T176T = SAP_T176T(spark)
    df_MANDT_FILTER_T176T = MANDT_FILTER_T176T(spark, df_SAP_T176T)
    LU_SAP_T176T(spark, df_MANDT_FILTER_T176T)
    df_SAP_TVV4T = SAP_TVV4T(spark)
    df_MANDT_FILTER_TVV4T = MANDT_FILTER_TVV4T(spark, df_SAP_TVV4T)
    LU_SAP_TVV4T(spark, df_MANDT_FILTER_TVV4T)
    df_SAP_TVV5T = SAP_TVV5T(spark)
    df_MANDT_FILTER_TVV5T = MANDT_FILTER_TVV5T(spark, df_SAP_TVV5T)
    LU_SAP_TVV5T(spark, df_MANDT_FILTER_TVV5T)
    df_SAP_TVLST = SAP_TVLST(spark)
    df_MANDT_FILTER_TVLST = MANDT_FILTER_TVLST(spark, df_SAP_TVLST)
    LU_SAP_TVLST(spark, df_MANDT_FILTER_TVLST)
    df_SAP_TSPAT = SAP_TSPAT(spark)
    df_MANDT_FILTER_TSPAT = MANDT_FILTER_TSPAT(spark, df_SAP_TSPAT)
    LU_SAP_TSPAT(spark, df_MANDT_FILTER_TSPAT)
    df_SAP_TVAUT = SAP_TVAUT(spark)
    df_MANDT_FILTER_TVAUT = MANDT_FILTER_TVAUT(spark, df_SAP_TVAUT)
    LU_SAP_TVAUT(spark, df_MANDT_FILTER_TVAUT)
    df_SAP_VBUK = SAP_VBUK(spark)
    df_SAP_VBAK = SAP_VBAK(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VBAK)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_GET_DUP = GET_DUP(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUP_FILTER = DUP_FILTER(spark, df_GET_DUP)
    df_MANDT_FILTER_1 = MANDT_FILTER_1(spark, df_SAP_VBUK)
    df_NEW_FIELDS_TRANSFORMATION_1 = NEW_FIELDS_TRANSFORMATION_1(spark, df_MANDT_FILTER_1)
    MD_SLS_ORDR(spark, df_SET_FIELD_ORDER_REFORMAT)

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
        "pipelines/SAP_01_MD_SLS_ORDR_BBA_BBL_BBN_BWI_TAI_GEU_HCS_MRS_P01_MBP_SVS_FSN"
    )
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/SAP_01_MD_SLS_ORDR_BBA_BBL_BBN_BWI_TAI_GEU_HCS_MRS_P01_MBP_SVS_FSN"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
