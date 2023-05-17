from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_dstn_chn_bba_bbl_bbn_bwi_geu_hcs_mbp_mrs_p01_tai_svs_hmd_hm2_atl_fsn.config.ConfigStore import *
from sap_01_md_matl_dstn_chn_bba_bbl_bbn_bwi_geu_hcs_mbp_mrs_p01_tai_svs_hmd_hm2_atl_fsn.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_dstn_chn_bba_bbl_bbn_bwi_geu_hcs_mbp_mrs_p01_tai_svs_hmd_hm2_atl_fsn.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_03_t179t = DS_SAP_03_t179t(spark)
    df_DS_SAP_03_t179t = collectMetrics(
        spark, 
        df_DS_SAP_03_t179t, 
        "graph", 
        "4lGPhcNh0VPhlNyxvz4uN$$lvqkl57I59o7UBLlgkn9s", 
        "zYOcFSRmhhfW94CrYd92L$$Dx0s1ZPxBkoHHcMVTpMTS"
    )
    df_MANDT_FILTER_t179t = MANDT_FILTER_t179t(spark, df_DS_SAP_03_t179t)
    LU_t179t(spark, df_MANDT_FILTER_t179t)
    df_DS_SAP_03_tvmst = DS_SAP_03_tvmst(spark)
    df_DS_SAP_03_tvmst = collectMetrics(
        spark, 
        df_DS_SAP_03_tvmst, 
        "graph", 
        "gUKnjceV_oUskzoT57LGm$$vLJDUqVmFQRDlrkiNrZdd", 
        "A5bRoQ1Gyavafl1q3scaC$$wYrNFuftUbNTlmtGN_plz"
    )
    df_MANDT_FILTER_tvmst = MANDT_FILTER_tvmst(spark, df_DS_SAP_03_tvmst)
    LU_tvmst(spark, df_MANDT_FILTER_tvmst)
    df_DS_SAP_04_tvms = DS_SAP_04_tvms(spark)
    df_DS_SAP_04_tvms = collectMetrics(
        spark, 
        df_DS_SAP_04_tvms, 
        "graph", 
        "U7nrx53myvMzgAtWNDf49$$jr0xjw5LaKf_nlvGO5W9a", 
        "h5KZ1yWysyvSNK7Ftz2RP$$DbqudD_qYAApaY95_DWKb"
    )
    df_MANDT_FILTER_tvms = MANDT_FILTER_tvms(spark, df_DS_SAP_04_tvms)
    LU_tvms(spark, df_MANDT_FILTER_tvms)
    df_DS_SAP_02_T179 = DS_SAP_02_T179(spark)
    df_DS_SAP_02_T179 = collectMetrics(
        spark, 
        df_DS_SAP_02_T179, 
        "graph", 
        "X6TqXG-PiEJyHY_LvYozj$$il7RW9i4DI0mLHXOpzcAJ", 
        "kqX9GWvvUo-GfKkwxDL0-$$qsyZVtDneSVJy1eyHGNva"
    )
    df_MANDT_FILTER_t179 = MANDT_FILTER_t179(spark, df_DS_SAP_02_T179)
    LU_t179(spark, df_MANDT_FILTER_t179)
    df_DS_SAP_01_MVKE = DS_SAP_01_MVKE(spark)
    df_DS_SAP_01_MVKE = collectMetrics(
        spark, 
        df_DS_SAP_01_MVKE, 
        "graph", 
        "d47JMjE5wgKv1mqAViKIo$$fu3Gx9WRBcmB52bsPdri5", 
        "UBwVO_QJdcVFzJwoZYTAb$$EbEjqySFvZtvz19p3lkkk"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_01_MVKE)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_ORDER = ORDER(spark, df_NEW_FIELDS)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_ORDER)
    df_DEDUPLICATE = collectMetrics(
        spark, 
        df_DEDUPLICATE, 
        "graph", 
        "qC9rwS1aMpeU4E3SX16fT$$Q3h_VtoL1N8xIGSKLzany", 
        "5Ba3KgeHPRa55p2PkRdH-$$qyWug9CK0mlajCNYBLipL"
    )
    MD_MATL_DSTN_CHN(spark, df_DEDUPLICATE)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_md_matl_dstn_chn")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_md_matl_dstn_chn")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
