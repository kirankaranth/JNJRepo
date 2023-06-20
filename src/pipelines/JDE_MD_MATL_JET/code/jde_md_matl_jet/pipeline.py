from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_jet.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_02_F0005 = DS_JDE_02_F0005(spark)
    df_DS_JDE_02_F0005 = collectMetrics(
        spark, 
        df_DS_JDE_02_F0005, 
        "graph", 
        "Gdh21fWIe0meYYKyOnx2w$$kpBKENmXN2tt3SPMie-r2", 
        "L7oq-OcgcWP2cQkadYTdI$$an8vkX3TfSc9bJADannAo"
    )
    df_Filter_1 = Filter_1(spark, df_DS_JDE_02_F0005)
    df_TRIM = TRIM(spark, df_Filter_1)
    df_FRAN_CD = FRAN_CD(spark, df_TRIM)
    FRAN_LU(spark, df_FRAN_CD)
    df_BRAVO_MINOR_DESC = BRAVO_MINOR_DESC(spark, df_TRIM)
    BRAVO_D_LU(spark, df_BRAVO_MINOR_DESC)
    df_MATL_TYPE_DESC = MATL_TYPE_DESC(spark, df_TRIM)
    MATL_T_LU(spark, df_MATL_TYPE_DESC)
    df_MATL_GRP = MATL_GRP(spark, df_TRIM)
    MATL_GR_LU(spark, df_MATL_GRP)
    df_MATL_GRP_2 = MATL_GRP_2(spark, df_TRIM)
    MATL_GR_2_LU(spark, df_MATL_GRP_2)
    df_F0005_NC = F0005_NC(spark)
    df_F0005_NC = collectMetrics(
        spark, 
        df_F0005_NC, 
        "graph", 
        "nS93-IpKKya1ivhyJS2Jc$$GW9Cm1rFo8CfN__9KYXrR", 
        "1CmECpvjpb-TFVSF23HNg$$VWqeqTcky5Fi2zyqw-M2O"
    )
    df_TRIM_1 = TRIM_1(spark, df_F0005_NC)
    df_MAT_SPEC = MAT_SPEC(spark, df_TRIM_1)
    MAT_SPEC_LU(spark, df_MAT_SPEC)
    df_T_O_MAT = T_O_MAT(spark, df_TRIM_1)
    T_O_MAT_LU(spark, df_T_O_MAT)
    df_DS_JDE_F554101B_NC = DS_JDE_F554101B_NC(spark)
    df_DS_JDE_F554101B_NC = collectMetrics(
        spark, 
        df_DS_JDE_F554101B_NC, 
        "graph", 
        "_ybg5H9hcJk4bK_aoxPKn$$CbIfKX2_jNCCamrHaBfPG", 
        "g7LzFHP4I6_lnxYYu4CZl$$Df7LseVuPSOlHSe83TuRK"
    )
    df_DEL_F = DEL_F(spark, df_DS_JDE_F554101B_NC)
    df_F554101B_SELEC = F554101B_SELEC(spark, df_DEL_F)
    df_T162 = T162(spark, df_F554101B_SELEC)
    df_SORT_VALUES = SORT_VALUES(spark, df_T162)
    df_DE_DUP = DE_DUP(spark, df_SORT_VALUES)
    df_T003 = T003(spark, df_F554101B_SELEC)
    df_SORT_VALUES_1 = SORT_VALUES_1(spark, df_T003)
    df_DE_DUP_1 = DE_DUP_1(spark, df_SORT_VALUES_1)
    df_RE_JOIN = RE_JOIN(spark, df_DE_DUP, df_DE_DUP_1)
    df_DS_JDE_01_F4101 = DS_JDE_01_F4101(spark)
    df_DS_JDE_01_F4101 = collectMetrics(
        spark, 
        df_DS_JDE_01_F4101, 
        "graph", 
        "KmcspySnyqS3B1jCwN5Wl$$fxxc3NjEQ-IkKVuSdMNK4", 
        "34drVHGEIPl8judmVhk_D$$5JpFkTtTM1UlfzRafC2Uj"
    )
    df_F4101_SELECTION = F4101_SELECTION(spark, df_DS_JDE_01_F4101)
    df_DEL = DEL(spark, df_F4101_SELECTION)
    df_JOIN = JOIN(spark, df_DEL, df_RE_JOIN)
    df_XFORM = XFORM(spark, df_JOIN)
    df_FIELD_ORDER = FIELD_ORDER(spark, df_XFORM)
    df_FIELD_ORDER = collectMetrics(
        spark, 
        df_FIELD_ORDER, 
        "graph", 
        "tEaJK5khO4iuuMg5fTf5n$$t5sw1Fvn9AEcqoSGAOuIE", 
        "X5_yIddhVngEUR5nfyn0E$$vfGy99rUws5XAaiMCPZQ0"
    )
    TARGET(spark, df_FIELD_ORDER)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL_JET")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MATL_JET")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
