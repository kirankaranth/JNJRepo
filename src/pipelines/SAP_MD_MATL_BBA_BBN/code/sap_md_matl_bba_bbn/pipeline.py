from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_bba_bbn.config.ConfigStore import *
from sap_md_matl_bba_bbn.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_bba_bbn.graph import *

def pipeline(spark: SparkSession) -> None:
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
    df_MAT_SPEC = MAT_SPEC(spark, df_CHARACTERISTICS)
    MAT_SPEC_LU(spark, df_MAT_SPEC)
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
    df_T023T = T023T(spark)
    df_T023T = collectMetrics(
        spark, 
        df_T023T, 
        "graph", 
        "Ch3aOlNjNkGAacOYx4_5f$$zzjTBKMZs4TUDsEgXVlMo", 
        "FCvUguins8xbfmUla0s1D$$J0wmVE1CoOS0EJdwLVj6w"
    )
    df_DEL_MANDT_9 = DEL_MANDT_9(spark, df_T023T)
    WGBEZx_LU(spark, df_DEL_MANDT_9)
    df_SPEC_VER = SPEC_VER(spark, df_CHARACTERISTICS)
    SPEC_VER_LU(spark, df_SPEC_VER)
    df_DS_SAP_MARA_BBA_BBN = DS_SAP_MARA_BBA_BBN(spark)
    df_DS_SAP_MARA_BBA_BBN = collectMetrics(
        spark, 
        df_DS_SAP_MARA_BBA_BBN, 
        "graph", 
        "FtbOy5VAM3PLdrv5DOdSX$$8TDSWjIIoR7sLz_ueyaFO", 
        "0pFTpMS3lywMyqdXzBawr$$TvJbXNoiQsX4up8G9YHqF"
    )
    df_DEL_MANDT_1 = DEL_MANDT_1(spark, df_DS_SAP_MARA_BBA_BBN)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_BBA_BBN")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_BBA_BBN")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
