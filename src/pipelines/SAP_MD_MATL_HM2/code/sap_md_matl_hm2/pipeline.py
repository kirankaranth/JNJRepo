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
    df_DS_SAP_MAKT_01 = DS_SAP_MAKT_01(spark)
    df_DS_SAP_MAKT_01 = collectMetrics(
        spark, 
        df_DS_SAP_MAKT_01, 
        "graph", 
        "1_LKabQ7stfOdICHxkWan$$tmBQjzhoGamT_eBR8_EaK", 
        "xR1qB0WHZ0SdHfft86-FR$$oxT476SvRJWV-7WJ6RwWp"
    )
    df_DEL_MANDT4 = DEL_MANDT4(spark, df_DS_SAP_MAKT_01)
    MAKTG_LU(spark, df_DEL_MANDT4)
    df_SUTUR_LEN = SUTUR_LEN(spark, df_CHARACTERISTICS)
    SUTUR_LEN_LU(spark, df_SUTUR_LEN)
    df_TSPAT = TSPAT(spark)
    df_TSPAT = collectMetrics(
        spark, 
        df_TSPAT, 
        "graph", 
        "Fxz52mHvigJCiGTtDShly$$A_d-qQSTnzK19sjyMY90j", 
        "hHscHRUv3NFgSWjzhGn3r$$ajZaM2NaV4KITq0gsue3m"
    )
    df_DEL_MANDT_6 = DEL_MANDT_6(spark, df_TSPAT)
    VTEXT_LU(spark, df_DEL_MANDT_6)
    df_T134T = T134T(spark)
    df_T134T = collectMetrics(
        spark, 
        df_T134T, 
        "graph", 
        "tEXn8WPj6NopoHRdUN2FJ$$cPEcqFbV3nRH3w7HuPiwo", 
        "ZYM8UAlA6PdKQTWs-lt2_$$LsowlDwrkUzbyvZvQyK6o"
    )
    df_DEL_MANDT_5 = DEL_MANDT_5(spark, df_T134T)
    MTBEZ_LU(spark, df_DEL_MANDT_5)
    df_STERILE = STERILE(spark, df_CHARACTERISTICS)
    STERILE_LU(spark, df_STERILE)
    df_BRAVO = BRAVO(spark, df_CHARACTERISTICS)
    BRAVO_LU(spark, df_BRAVO)
    MAKTX_LU(spark, df_DEL_MANDT4)
    df_NDL_SLS_TYPE = NDL_SLS_TYPE(spark, df_CHARACTERISTICS)
    NDL_SLS_LU(spark, df_NDL_SLS_TYPE)
    df_CAWNT = CAWNT(spark)
    df_CAWNT = collectMetrics(
        spark, 
        df_CAWNT, 
        "graph", 
        "lw57OayKYQQpq-wv8BwYz$$ZETNvifMTrIl3Xqoh_U9O", 
        "fVAdaZ17mmn9N9z25Gq-J$$fjH3D0n9k7HgZfus165e7"
    )
    df_DEL_MANDT_8 = DEL_MANDT_8(spark, df_CAWNT)
    df_BRAVO_DESC = BRAVO_DESC(spark, df_BRAVO, df_DEL_MANDT_8)
    ATWTB_LU(spark, df_BRAVO_DESC)
    df_NDL_ALLOY = NDL_ALLOY(spark, df_CHARACTERISTICS)
    NDL_ALLOY_LU(spark, df_NDL_ALLOY)
    df_T023T = T023T(spark)
    df_T023T = collectMetrics(
        spark, 
        df_T023T, 
        "graph", 
        "721hJuTpWfcK1pilBZsCt$$8kVPKBlTwV_EHgzhUqJ8p", 
        "2NDEoBs_3_mHkIDt_1Jhg$$pYUBdgKQZNpS4whPIjuQ5"
    )
    df_DEL_MANDT_9 = DEL_MANDT_9(spark, df_T023T)
    WGBEZx_LU(spark, df_DEL_MANDT_9)
    OBJEK_LU(spark, df_INOB_DEDUP)
    df_NDL_COLOR = NDL_COLOR(spark, df_CHARACTERISTICS)
    NDL_COL_LU(spark, df_NDL_COLOR)
    df_T006A = T006A(spark)
    df_T006A = collectMetrics(
        spark, 
        df_T006A, 
        "graph", 
        "nR5N0Jtgo0SMP_AvBFdRp$$AFYAZXzg_nw4dr4nEO75k", 
        "93TiFAMOCOU-nvZZBDgQW$$0QIYt9N5wZHp0Js8Od0yX"
    )
    df_DEL_MANDT_7 = DEL_MANDT_7(spark, df_T006A)
    MSEHL_LU(spark, df_DEL_MANDT_7)
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
    df_XFORM = XFORM(spark, df_DEL_MANDT_1)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    df_SELECT_FIELDS = collectMetrics(
        spark, 
        df_SELECT_FIELDS, 
        "graph", 
        "5tjEC2R_fitFg858EKn_H$$8zrgDqoYbVtMNO-GK4V6_", 
        "bPAwoIg9wa3XArlXP-7m_$$c1QUVrlfKTD9A8sObAKfS"
    )
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
