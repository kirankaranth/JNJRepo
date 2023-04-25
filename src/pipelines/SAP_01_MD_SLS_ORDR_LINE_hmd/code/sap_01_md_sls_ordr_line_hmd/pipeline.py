from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_sls_ordr_line_hmd.config.ConfigStore import *
from sap_01_md_sls_ordr_line_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_sls_ordr_line_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_TVRO = SAP_TVRO(spark)
    df_SAP_TVRO = collectMetrics(
        spark, 
        df_SAP_TVRO, 
        "graph", 
        "jpkTuAMVhuIm_eQDDp8Ba$$8lG2JYhbvH3klHQ2RXFf2", 
        "AAvLr6KnTKyXLQVbfm5rp$$OMsJUwdgWCExUyJLN6DYL"
    )
    df_MANDT_FILTER_1_1_1_2 = MANDT_FILTER_1_1_1_2(spark, df_SAP_TVRO)
    LU_SAP_TVRO(spark, df_MANDT_FILTER_1_1_1_2)
    df_SAP_TVAGT = SAP_TVAGT(spark)
    df_SAP_TVAGT = collectMetrics(
        spark, 
        df_SAP_TVAGT, 
        "graph", 
        "Q0qKeXnuCrLITHHHQYcPD$$wC8E90Bay68x2xbd3vPza", 
        "haywR6eBzZMs2o5Rc3Vqw$$aiExV_JycoIWVQgvq84Qg"
    )
    df_MANDT_FILTER_1_1_1_1 = MANDT_FILTER_1_1_1_1(spark, df_SAP_TVAGT)
    LU_SAP_TVAGT(spark, df_MANDT_FILTER_1_1_1_1)
    df_SAP_TVM3T = SAP_TVM3T(spark)
    df_SAP_TVM3T = collectMetrics(
        spark, 
        df_SAP_TVM3T, 
        "graph", 
        "MpQRcomNC5OPLgtDddBM2$$eDcpp6GQiL9jc7g3KYtgP", 
        "gQV7l8XQEtk_g8_4Kj4BA$$h1oyDZ0yvRbYTCSkBMjHX"
    )
    df_MANDT_FILTER_1_1_1_2_1_1_1 = MANDT_FILTER_1_1_1_2_1_1_1(spark, df_SAP_TVM3T)
    LU_SAP_TVM3T(spark, df_MANDT_FILTER_1_1_1_2_1_1_1)
    df_SAP_TVM4T = SAP_TVM4T(spark)
    df_SAP_TVM4T = collectMetrics(
        spark, 
        df_SAP_TVM4T, 
        "graph", 
        "RqHIe54arPVcNWrlF6uEk$$x9g_K8MwJNtqhHa7e7Etq", 
        "FNx0Q-Ltztj4XU8X4CJzQ$$y7ZFZ2va_i0iVy94IUKFX"
    )
    df_MANDT_FILTER_TVM4T = MANDT_FILTER_TVM4T(spark, df_SAP_TVM4T)
    LU_SAP_TVM4T(spark, df_MANDT_FILTER_TVM4T)
    df_SAP_TVM1T = SAP_TVM1T(spark)
    df_SAP_TVM1T = collectMetrics(
        spark, 
        df_SAP_TVM1T, 
        "graph", 
        "j0D_3Dye54ZpY6_uDH1s4$$gr97rgle_BlO3KQYw_Lkt", 
        "1W-jLpg5ZGO-wyfUU_Q8F$$qX-ts8m5lxn0paH7qtZFf"
    )
    df_MANDT_FILTER_1_1_1_2_1 = MANDT_FILTER_1_1_1_2_1(spark, df_SAP_TVM1T)
    LU_SAP_TVM1T(spark, df_MANDT_FILTER_1_1_1_2_1)
    df_SAP_TVM2T = SAP_TVM2T(spark)
    df_SAP_TVM2T = collectMetrics(
        spark, 
        df_SAP_TVM2T, 
        "graph", 
        "CTnu9SCfzGyxC8R66njfk$$JksrogOi-ZhdiWOPZZSlC", 
        "AmGDZroNL-jsDDqfsrWZh$$4BVHsky9iM22Y-t4J7dEB"
    )
    df_MANDT_FILTER_1_1_1_2_1_1 = MANDT_FILTER_1_1_1_2_1_1(spark, df_SAP_TVM2T)
    LU_SAP_TVM2T(spark, df_MANDT_FILTER_1_1_1_2_1_1)
    df_SAP_TVAPT = SAP_TVAPT(spark)
    df_SAP_TVAPT = collectMetrics(
        spark, 
        df_SAP_TVAPT, 
        "graph", 
        "09TojS3CKVBxjIXqcUEnI$$2mKKi1t69_BI0gShQRQOU", 
        "XmHSfPaSpG7n027K81vlJ$$WOJCu5ZUOn7G7iRu_Scy8"
    )
    df_MANDT_FILTER_1_1 = MANDT_FILTER_1_1(spark, df_SAP_TVAPT)
    LU_SAP_TVAPT(spark, df_MANDT_FILTER_1_1)
    df_SAP_TVM5T = SAP_TVM5T(spark)
    df_SAP_TVM5T = collectMetrics(
        spark, 
        df_SAP_TVM5T, 
        "graph", 
        "PL1yDJS6pGY4yQW6n3SZ0$$E5IypcbxKkhozIwpxTB0S", 
        "YKKNqM6FA079LIF9pWWvP$$1ZNT6lyslvPuoki_NeMwZ"
    )
    df_MANDT_FILTER_1_1_1_2_1_1_1_1 = MANDT_FILTER_1_1_1_2_1_1_1_1(spark, df_SAP_TVM5T)
    LU_SAP_TVM5T(spark, df_MANDT_FILTER_1_1_1_2_1_1_1_1)
    df_DS_SAP_01_TVROT = DS_SAP_01_TVROT(spark)
    df_DS_SAP_01_TVROT = collectMetrics(
        spark, 
        df_DS_SAP_01_TVROT, 
        "graph", 
        "6q7t-WcM9nRVSEJIO0V_l$$KxW9FjKEDJ0n5QuAo6Ba5", 
        "hs0TSKMHpMbl7iofxo6Dd$$3P43oio6utN47V4Xyf2AK"
    )
    df_SAP_VBAP = SAP_VBAP(spark)
    df_SAP_VBAP = collectMetrics(
        spark, 
        df_SAP_VBAP, 
        "graph", 
        "aLq-bTfxkKYLQ-C3qrbns$$AdFi-f_5eIzBc9bo111G3", 
        "-5sicrluGo4KP6YxY4ZfA$$8KntWIgxbxcywF5SdcD2h"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VBAP)
    df_SAP_VBAK = SAP_VBAK(spark)
    df_SAP_VBAK = collectMetrics(
        spark, 
        df_SAP_VBAK, 
        "graph", 
        "V5RlhzURZ3AovBH4EtGoR$$RzAs4L5PxhIDJaZn_M0iF", 
        "sjH_LLGBfigv0q-MWiqp1$$G22L17gIO1yk7wrAFjV7H"
    )
    df_MANDT_FILTER_1 = MANDT_FILTER_1(spark, df_SAP_VBAK)
    df_SAP_VBKD = SAP_VBKD(spark)
    df_SAP_VBKD = collectMetrics(
        spark, 
        df_SAP_VBKD, 
        "graph", 
        "-IVLYiizxuJsVaNAApNn-$$FUWMauCCyRzCP3mv6bnws", 
        "Vrb1zO3XQx8eG-gwIAYs3$$l__PwkEUPq7stwQbww9t3"
    )
    df_MANDT_FILTER_1_1_1 = MANDT_FILTER_1_1_1(spark, df_SAP_VBKD)
    df_SAP_TVST = SAP_TVST(spark)
    df_SAP_TVST = collectMetrics(
        spark, 
        df_SAP_TVST, 
        "graph", 
        "EcpzPcN7OZ3-aIOhKZX8q$$WeFDvfTHhZP5iSZh4X2dj", 
        "GL2Xa0W0jHtvhi96Cv9d3$$pQlwjl9oAei-fgVZHhElN"
    )
    df_MANDT_FILTER_1_1_1_2_1_1_1_1_1_1 = MANDT_FILTER_1_1_1_2_1_1_1_1_1_1(spark, df_SAP_TVST)
    df_Join_1 = Join_1(
        spark, 
        df_MANDT_FILTER, 
        df_MANDT_FILTER_1, 
        df_MANDT_FILTER_1_1_1, 
        df_MANDT_FILTER_1_1_1_2_1_1_1_1_1_1
    )
    df_NEW_FIEDS = NEW_FIEDS(spark, df_Join_1)
    df_NEW_FIEDS = collectMetrics(
        spark, 
        df_NEW_FIEDS, 
        "graph", 
        "trr1rRj4G8lOWThSOk6kL$$xkqswg8HFqTEQPLufsCpq", 
        "CNEOYEnXaJkgZWU69c91w$$iKg9J2RNdLawE5-USfbrU"
    )
    MD_SLS_ORDR_LINE(spark, df_NEW_FIEDS)
    df_Filter_TVROT = Filter_TVROT(spark, df_DS_SAP_01_TVROT)
    LU_SAP_TVROT(spark, df_Filter_TVROT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_NEW_FIEDS)
    df_DUPLICATE_CHECK_1 = DUPLICATE_CHECK_1(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_1 = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_1, 
        "graph", 
        "YAEbc2EldJEFTGcdjvCas$$xTsLyG69sAy2_8LqHLiqY", 
        "u5hH9ARIFfKA3b9R3UO_P$$g2_MLeySKbeRXSk-UH5XK"
    )
    df_DUPLICATE_CHECK_1.cache().count()
    df_DUPLICATE_CHECK_1.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_SLS_ORDR_LINE_hmd")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_SLS_ORDR_LINE_hmd")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
