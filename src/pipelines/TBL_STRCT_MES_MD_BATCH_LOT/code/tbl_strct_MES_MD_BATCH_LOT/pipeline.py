from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MES_MD_BATCH_LOT.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MES_MD_CNTNR = sql_MES_MD_CNTNR(spark)
    df_sql_MES_MD_MATL_ISS_HIST = sql_MES_MD_MATL_ISS_HIST(spark)
    MES_MD_MATL_ISS_HIST(spark, df_sql_MES_MD_MATL_ISS_HIST)
    df_sql_MES_MD_CNTNR_DTL = sql_MES_MD_CNTNR_DTL(spark)
    MES_MD_CNTNR_DTL(spark, df_sql_MES_MD_CNTNR_DTL)
    df_sql_MES_MD_MFG_ORDR_STS = sql_MES_MD_MFG_ORDR_STS(spark)
    MES_MD_MFG_ORDR_STS(spark, df_sql_MES_MD_MFG_ORDR_STS)
    MES_MD_CNTNR(spark, df_sql_MES_MD_CNTNR)
    df_sql_MES_MD_DEFCT_HIST = sql_MES_MD_DEFCT_HIST(spark)
    df_sql_MES_MD_COLL_SAMP_SPOOL_HIS = sql_MES_MD_COLL_SAMP_SPOOL_HIS(spark)
    MES_MD_COLL_SAMP_SPOOL_HIS(spark, df_sql_MES_MD_COLL_SAMP_SPOOL_HIS)
    df_sql_MES_MD_CNTNR_ASSN_HIST = sql_MES_MD_CNTNR_ASSN_HIST(spark)
    MES_MD_CNTNR_ASSN_HIST(spark, df_sql_MES_MD_CNTNR_ASSN_HIST)
    df_sql_MES_MD_MFG_CNTNR_PRIR = sql_MES_MD_MFG_CNTNR_PRIR(spark)
    MES_MD_MFG_CNTNR_PRIR(spark, df_sql_MES_MD_MFG_CNTNR_PRIR)
    df_sql_MES_MD_ISS_HIST_DTL = sql_MES_MD_ISS_HIST_DTL(spark)
    df_sql_MES_MD_EXEQ_TASK_HIST = sql_MES_MD_EXEQ_TASK_HIST(spark)
    df_sql_MES_MD_CMPNT_ISS_HIST = sql_MES_MD_CMPNT_ISS_HIST(spark)
    MES_MD_CMPNT_ISS_HIST(spark, df_sql_MES_MD_CMPNT_ISS_HIST)
    df_sql_MES_MD_CNTNR_STS = sql_MES_MD_CNTNR_STS(spark)
    MES_MD_CNTNR_STS(spark, df_sql_MES_MD_CNTNR_STS)
    df_sql_MES_MD_MATL_MOVE_HIST = sql_MES_MD_MATL_MOVE_HIST(spark)
    MES_MD_MATL_MOVE_HIST(spark, df_sql_MES_MD_MATL_MOVE_HIST)
    df_sql_MES_MD_HIST_MNLINE = sql_MES_MD_HIST_MNLINE(spark)
    df_sql_MES_MD_CNTNR_DEFCT_HIST_DT = sql_MES_MD_CNTNR_DEFCT_HIST_DT(spark)
    MES_MD_CNTNR_DEFCT_HIST_DT(spark, df_sql_MES_MD_CNTNR_DEFCT_HIST_DT)
    df_sql_MES_MD_ESIGN_HIST_SUM = sql_MES_MD_ESIGN_HIST_SUM(spark)
    MES_MD_ESIGN_HIST_SUM(spark, df_sql_MES_MD_ESIGN_HIST_SUM)
    df_sql_MES_MD_FDR_SPOOL_TXN_HIST = sql_MES_MD_FDR_SPOOL_TXN_HIST(spark)
    MES_MD_HIST_MNLINE(spark, df_sql_MES_MD_HIST_MNLINE)
    df_sql_MES_MD_BTCH_STRT_HIST_DTL = sql_MES_MD_BTCH_STRT_HIST_DTL(spark)
    MES_MD_BTCH_STRT_HIST_DTL(spark, df_sql_MES_MD_BTCH_STRT_HIST_DTL)
    df_sql_MES_MD_CNTNR_ASSN_HIST_CHI = sql_MES_MD_CNTNR_ASSN_HIST_CHI(spark)
    MES_MD_CNTNR_ASSN_HIST_CHI(spark, df_sql_MES_MD_CNTNR_ASSN_HIST_CHI)
    MES_MD_EXEQ_TASK_HIST(spark, df_sql_MES_MD_EXEQ_TASK_HIST)
    df_sql_MES_MD_MFG_ORDR = sql_MES_MD_MFG_ORDR(spark)
    MES_MD_DEFCT_HIST(spark, df_sql_MES_MD_DEFCT_HIST)
    df_sql_MES_MD_MATL_QTY_HIST = sql_MES_MD_MATL_QTY_HIST(spark)
    MES_MD_MATL_QTY_HIST(spark, df_sql_MES_MD_MATL_QTY_HIST)
    df_sql_MES_MD_MATL_MOVE_IN_HIST = sql_MES_MD_MATL_MOVE_IN_HIST(spark)
    MES_MD_MATL_MOVE_IN_HIST(spark, df_sql_MES_MD_MATL_MOVE_IN_HIST)
    df_sql_MES_MD_REMV_HIST_DTL = sql_MES_MD_REMV_HIST_DTL(spark)
    df_sql_MES_MD_MATL_QTY_HIST_DTL = sql_MES_MD_MATL_QTY_HIST_DTL(spark)
    df_sql_MES_MD_HIST_XREF = sql_MES_MD_HIST_XREF(spark)
    MES_MD_REMV_HIST_DTL(spark, df_sql_MES_MD_REMV_HIST_DTL)
    df_sql_MES_MD_CNTNR_SPLT_HIST_DTL = sql_MES_MD_CNTNR_SPLT_HIST_DTL(spark)
    MES_MD_CNTNR_SPLT_HIST_DTL(spark, df_sql_MES_MD_CNTNR_SPLT_HIST_DTL)
    df_sql_MES_MD_CNTNR_SPLT_HIST = sql_MES_MD_CNTNR_SPLT_HIST(spark)
    MES_MD_CNTNR_SPLT_HIST(spark, df_sql_MES_MD_CNTNR_SPLT_HIST)
    df_sql_MES_MD_COLL_DATA_HIST = sql_MES_MD_COLL_DATA_HIST(spark)
    MES_MD_COLL_DATA_HIST(spark, df_sql_MES_MD_COLL_DATA_HIST)
    MES_MD_ISS_HIST_DTL(spark, df_sql_MES_MD_ISS_HIST_DTL)
    df_sql_MES_MD_ESIGN_MEANING = sql_MES_MD_ESIGN_MEANING(spark)
    MES_MD_ESIGN_MEANING(spark, df_sql_MES_MD_ESIGN_MEANING)
    MES_MD_MFG_ORDR(spark, df_sql_MES_MD_MFG_ORDR)
    df_sql_MES_MD_ESIGN_HIST_DTL = sql_MES_MD_ESIGN_HIST_DTL(spark)
    MES_MD_HIST_XREF(spark, df_sql_MES_MD_HIST_XREF)
    MES_MD_FDR_SPOOL_TXN_HIST(spark, df_sql_MES_MD_FDR_SPOOL_TXN_HIST)
    MES_MD_ESIGN_HIST_DTL(spark, df_sql_MES_MD_ESIGN_HIST_DTL)
    MES_MD_MATL_QTY_HIST_DTL(spark, df_sql_MES_MD_MATL_QTY_HIST_DTL)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MES_MD_BATCH_LOT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MES_MD_BATCH_LOT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
