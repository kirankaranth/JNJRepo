from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_hm2.config.ConfigStore import *
from sap_md_matl_hm2.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_hm2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_AUSP = AUSP(spark)
    df_AUSP = collectMetrics(
        spark, 
        df_AUSP, 
        "graph", 
        "v1n3bmO0KowRNFFTEKB8J$$d5J99CsIScvPVeR0sgJQK", 
        "yrj1p71IWjwcYC4crq5v1$$ofGlqAmNNgYIu5q1wULoY"
    )
    df_DEL_MANDT = DEL_MANDT(spark, df_AUSP)
    df_CABN = CABN(spark)
    df_CABN = collectMetrics(
        spark, 
        df_CABN, 
        "graph", 
        "DkQaAQa_MeJI_NSPofPnn$$mWhnyQZtLhIq3sso1TlT9", 
        "FSV4VbuSJCLF2YVckAWpK$$l0vsh63QQZ_WVJisfvF2o"
    )
    df_DEL_MANDT2 = DEL_MANDT2(spark, df_CABN)
    df_INOB = INOB(spark)
    df_INOB = collectMetrics(
        spark, 
        df_INOB, 
        "graph", 
        "6Ln3EE1WjbsC9M4tgw9oY$$mTunW5ltbj5plJZdQVwlN", 
        "rM-i923U7SU0ZtNzVl2qx$$rnxTxry3Com38WVhi_poM"
    )
    df_D_M_OBTAB_KLART = D_M_OBTAB_KLART(spark, df_INOB)
    df_INOB_DEDUP = INOB_DEDUP(spark, df_D_M_OBTAB_KLART)
    df_CHARACTERISTICS = CHARACTERISTICS(spark, df_DEL_MANDT, df_INOB_DEDUP, df_DEL_MANDT2)
    df_SUTUR_USP = SUTUR_USP(spark, df_CHARACTERISTICS)
    SUTUR_USP_LU(spark, df_SUTUR_USP)
    df_SUTUR_LEN = SUTUR_LEN(spark, df_CHARACTERISTICS)
    SUTUR_LEN_LU(spark, df_SUTUR_LEN)
    df_T134T = T134T(spark)
    df_T134T = collectMetrics(
        spark, 
        df_T134T, 
        "graph", 
        "tEXn8WPj6NopoHRdUN2FJ$$cPEcqFbV3nRH3w7HuPiwo", 
        "ZYM8UAlA6PdKQTWs-lt2_$$LsowlDwrkUzbyvZvQyK6o"
    )
    df_DEL_MANDT4_1 = DEL_MANDT4_1(spark, df_T134T)
    T134T_LU(spark, df_DEL_MANDT4_1)
    df_STERILE = STERILE(spark, df_CHARACTERISTICS)
    STERILE_LU(spark, df_STERILE)
    df_BRAVO = BRAVO(spark, df_CHARACTERISTICS)
    BRAVO_LU(spark, df_BRAVO)
    df_DS_SAP_MAKT_01 = DS_SAP_MAKT_01(spark)
    df_DS_SAP_MAKT_01 = collectMetrics(
        spark, 
        df_DS_SAP_MAKT_01, 
        "graph", 
        "1_LKabQ7stfOdICHxkWan$$tmBQjzhoGamT_eBR8_EaK", 
        "xR1qB0WHZ0SdHfft86-FR$$oxT476SvRJWV-7WJ6RwWp"
    )
    df_DEL_MANDT4 = DEL_MANDT4(spark, df_DS_SAP_MAKT_01)
    MAKTX_LU(spark, df_DEL_MANDT4)
    df_NDL_SLS_TYPE = NDL_SLS_TYPE(spark, df_CHARACTERISTICS)
    NDL_SLS_LU(spark, df_NDL_SLS_TYPE)
    df_NDL_ALLOY = NDL_ALLOY(spark, df_CHARACTERISTICS)
    NDL_ALLOY_LU(spark, df_NDL_ALLOY)
    OBJEK_LU(spark, df_INOB_DEDUP)
    df_NDL_COLOR = NDL_COLOR(spark, df_CHARACTERISTICS)
    NDL_COL_LU(spark, df_NDL_COLOR)
    df_MAT_TYPE = MAT_TYPE(spark, df_CHARACTERISTICS)
    MAT_TYPE_LU(spark, df_MAT_TYPE)
    df_DS_SAP_MARA_HM2 = DS_SAP_MARA_HM2(spark)
    df_DS_SAP_MARA_HM2 = collectMetrics(
        spark, 
        df_DS_SAP_MARA_HM2, 
        "graph", 
        "HfNO990ZZuTw923N8tVWK$$EFcDwi0OfOHsZsJmPxZbx", 
        "NE3et2tjx-fVp4f4QwZ0q$$Ev8jIzJ8jt_9NrE7XnO6x"
    )
    df_DEL_MANDT_1 = DEL_MANDT_1(spark, df_DS_SAP_MARA_HM2)
    df_DEL_MANDT3 = DEL_MANDT3(spark, df_DEL_MANDT_1)
    df_XFORM = XFORM(spark, df_DEL_MANDT3)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    df_SELECT_FIELDS = collectMetrics(
        spark, 
        df_SELECT_FIELDS, 
        "graph", 
        "5tjEC2R_fitFg858EKn_H$$8zrgDqoYbVtMNO-GK4V6_", 
        "bPAwoIg9wa3XArlXP-7m_$$c1QUVrlfKTD9A8sObAKfS"
    )
    df_TSPAT = TSPAT(spark)
    df_TSPAT = collectMetrics(
        spark, 
        df_TSPAT, 
        "graph", 
        "Fxz52mHvigJCiGTtDShly$$A_d-qQSTnzK19sjyMY90j", 
        "hHscHRUv3NFgSWjzhGn3r$$ajZaM2NaV4KITq0gsue3m"
    )
    TARGET(spark, df_SELECT_FIELDS)
    df_DEL_MANDT4_1_1 = DEL_MANDT4_1_1(spark, df_TSPAT)
    df_DEL_MANDT4_1_1 = collectMetrics(
        spark, 
        df_DEL_MANDT4_1_1, 
        "graph", 
        "DB1-X1UNUjNrATmoHHZ9s$$2UejoDAr0DDHgtmkPagIo", 
        "mG9jjAN_aHlKbl1hDoxu0$$vSS9A4FmuifJX-qSt52BS"
    )
    df_DEL_MANDT4_1_1.cache().count()
    df_DEL_MANDT4_1_1.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_HM2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_HM2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
