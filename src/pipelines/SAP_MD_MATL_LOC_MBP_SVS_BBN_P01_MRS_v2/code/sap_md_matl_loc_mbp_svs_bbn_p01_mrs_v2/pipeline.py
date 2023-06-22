from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs_v2.config.ConfigStore import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs_v2.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs_v2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_T024 = SAP_T024(spark)
    df_SAP_T024 = collectMetrics(
        spark, 
        df_SAP_T024, 
        "graph", 
        "HT2HjrosBKt_E4DklHJ0A$$nGcXahxTkxbJHjpQd_R06", 
        "7yA9bxsafHKSBveA-sRsZ$$31FXL2RMD35Ak6VXX0W0l"
    )
    df_MANDT_FILTER_T024 = MANDT_FILTER_T024(spark, df_SAP_T024)
    LU_SAP_T024(spark, df_MANDT_FILTER_T024)
    df_SAP_T024D = SAP_T024D(spark)
    df_SAP_T024D = collectMetrics(
        spark, 
        df_SAP_T024D, 
        "graph", 
        "15SNdkpCg87acS-k9ne2R$$b2Wm_WjJbodKylZlhxSgW", 
        "esIIcjL67_j1r9OKCDH0t$$dX9Rb3m1s5ULNRpEHnHi_"
    )
    df_MANDT_FILTER_T024D = MANDT_FILTER_T024D(spark, df_SAP_T024D)
    LU_SAP_T024D(spark, df_MANDT_FILTER_T024D)
    df_SAP_T141T = SAP_T141T(spark)
    df_SAP_T141T = collectMetrics(
        spark, 
        df_SAP_T141T, 
        "graph", 
        "1FWkPms6MxDpajLybOfNc$$Ew_f7FLqCPhp6vT_kd9Sd", 
        "U89ocgcqrx-2I93Il0vEN$$zScOfFzRlARd2_axWtsg2"
    )
    df_MANDT_FILTER_T141T = MANDT_FILTER_T141T(spark, df_SAP_T141T)
    LU_SAP_T141T(spark, df_MANDT_FILTER_T141T)
    df_SAP_T460A = SAP_T460A(spark)
    df_SAP_T460A = collectMetrics(
        spark, 
        df_SAP_T460A, 
        "graph", 
        "uDDE2XeJ2B-M5rdb0GaKq$$omoo0WKYJAhJwXGVi0tGt", 
        "pWdt2NQCoC7fVBQUERL0z$$vVH8d9NQwE9Pul9Kv1HdU"
    )
    df_MANDT_FILTER_T141T_1 = MANDT_FILTER_T141T_1(spark, df_SAP_T460A)
    LU_SAP_T460A(spark, df_MANDT_FILTER_T141T_1)
    df_SAP_MARC = SAP_MARC(spark)
    df_SAP_MARC = collectMetrics(
        spark, 
        df_SAP_MARC, 
        "graph", 
        "qBaLyQXuPwT_Wnnr-7Ag0$$2NVc94nCKfKYjJ8bvE9vS", 
        "v4y0zIcNvP1nBj3KxyWjD$$GKS6ltO2yraJj8nGcHGQF"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_MARC)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "fjDpZN9j4pw5z4FK3xnJe$$qckqpvmuTyiheJtG62wFT", 
        "moIjCFtHMLSTNbIUFn6Ii$$-SMRHL8WUlI1FizN3ewKa"
    )
    df_GET_DUP = GET_DUP(spark, df_SET_FIELD_ORDER_REFORMAT)
    MD_MATL_LOC(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUP_FILTER = DUP_FILTER(spark, df_GET_DUP)
    df_DUP_FILTER = collectMetrics(
        spark, 
        df_DUP_FILTER, 
        "graph", 
        "sCyF8TNo7xjBB3V-16N_6$$jibaXAiMa9PvlgehCaI3Y", 
        "4QQ1Zq5XxtmBTnO2FAG14$$St1BIVF8CsrGD96C8Mf2I"
    )
    df_DUP_FILTER.cache().count()
    df_DUP_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_LOC_MBP_SVS_BBN_P01_MRS_v2")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_LOC_MBP_SVS_BBN_P01_MRS_v2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
