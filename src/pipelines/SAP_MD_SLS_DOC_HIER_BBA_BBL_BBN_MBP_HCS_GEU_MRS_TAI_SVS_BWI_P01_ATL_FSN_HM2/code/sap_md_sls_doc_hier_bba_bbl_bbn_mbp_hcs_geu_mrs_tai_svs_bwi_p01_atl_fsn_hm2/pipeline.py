from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn_hm2.config.ConfigStore import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn_hm2.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn_hm2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_VBFA = SAP_VBFA(spark)
    df_SAP_VBFA = collectMetrics(
        spark, 
        df_SAP_VBFA, 
        "graph", 
        "4XXaxtu9sMF3IzN18du4Z$$YccSwnO-QUsEx100lxB6n", 
        "lbrEqZphGAu7THDyw-yT9$$hq_asf4UoCvPk8pNd4-3M"
    )
    df_SAP_VBAK = SAP_VBAK(spark)
    df_SAP_VBAK = collectMetrics(
        spark, 
        df_SAP_VBAK, 
        "graph", 
        "gH8xq7AxEv_OsMubnhIaX$$WiZ3Q0s6nLf2_knirrLmp", 
        "jS9O9fWnEbd-eodGmNryH$$BQwyrzBx87lCxFpWnnBOe"
    )
    df_MANDT_FILTER_VBAK = MANDT_FILTER_VBAK(spark, df_SAP_VBAK)
    df_FIELDS_VBAK = FIELDS_VBAK(spark, df_MANDT_FILTER_VBAK)
    df_MANDT_FILTER_VBFA = MANDT_FILTER_VBFA(spark, df_SAP_VBFA)
    df_FIELDS_VBFA = FIELDS_VBFA(spark, df_MANDT_FILTER_VBFA)
    df_JOIN_SAP = JOIN_SAP(spark, df_FIELDS_VBAK, df_FIELDS_VBFA)
    df_JOIN_SAP_1 = JOIN_SAP_1(spark, df_JOIN_SAP)
    df_JOIN_SAP_1 = collectMetrics(
        spark, 
        df_JOIN_SAP_1, 
        "graph", 
        "oI-FE2_llgcpAO-2AfNZx$$CrpmTDk12bcjEBQRLwTxV", 
        "7EgaobHDyHArNbi2wMPVn$$ZmV5aFjWfL3mRbNO5_TeH"
    )
    df_JOIN_SAP_1.cache().count()
    df_JOIN_SAP_1.unpersist()
    df_SAP_VBAP = SAP_VBAP(spark)
    df_SAP_VBAP = collectMetrics(
        spark, 
        df_SAP_VBAP, 
        "graph", 
        "bZiAbnYCSIO7GsxM1WWHs$$gIusxIZBWj-wYQlnihBKR", 
        "o5YEKFinGSrT92hZqv_x1$$uu6ZzCgA_RuPlXQug5a1D"
    )
    df_MANDT_FILTER_VBAP = MANDT_FILTER_VBAP(spark, df_SAP_VBAP)
    df_Reformat_1 = Reformat_1(spark, df_MANDT_FILTER_VBAP)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "graph", 
        "iRY1xc-9_1b6lpgR-4dq_$$xMLEACaAsRuEaKYV2pgGA", 
        "ePJN1ygwAzsgETyeGclvu$$J7Yju6ImP7wKWMvjwRIP6"
    )
    df_Reformat_1.cache().count()
    df_Reformat_1.unpersist()

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
    spark.conf.set(
        "prophecy.metadata.pipeline.uri",
        "pipelines/SAP_MD_SLS_DOC_HIER_BBA_BBL_BBN_MBP_HCS_GEU_MRS_TAI_SVS_BWI_P01_ATL_FSN_HM2"
    )
    registerUDFs(spark)
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/SAP_MD_SLS_DOC_HIER_BBA_BBL_BBN_MBP_HCS_GEU_MRS_TAI_SVS_BWI_P01_ATL_FSN_HM2"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
