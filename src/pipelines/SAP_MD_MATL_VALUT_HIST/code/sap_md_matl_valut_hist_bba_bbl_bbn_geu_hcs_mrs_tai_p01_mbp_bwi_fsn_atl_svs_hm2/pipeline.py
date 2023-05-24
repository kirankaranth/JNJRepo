from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hm2.config.ConfigStore import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hm2.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hm2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_MBEWH = DS_SAP_01_MBEWH(spark)
    df_DS_SAP_01_MBEWH = collectMetrics(
        spark, 
        df_DS_SAP_01_MBEWH, 
        "graph", 
        "cxoCuDHwAQVV9qAxaDLKI$$Tl4xcoH6KgRgjEmXm92lI", 
        "Oscv_qhLxsQIK_Vv9ITnp$$PJ4FDPzWeiVCvgfd1K8Ix"
    )
    df_DS_SAP_02_MARA = DS_SAP_02_MARA(spark)
    df_DS_SAP_02_MARA = collectMetrics(
        spark, 
        df_DS_SAP_02_MARA, 
        "graph", 
        "Pc0An3n_ib9Adm1hpUcW6$$a47BTAfnZe86y3h-rbG1X", 
        "SZCF7qX6TbjS4EeicJJIE$$o8iRpzGk1mKBDAQ0Oivig"
    )
    df_MANDT_FILTER_MARA = MANDT_FILTER_MARA(spark, df_DS_SAP_02_MARA)
    df_Reformat_MARA = Reformat_MARA(spark, df_MANDT_FILTER_MARA)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_01_MBEWH)
    df_Join_1 = Join_1(spark, df_MANDT_FILTER, df_Reformat_MARA)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_ORDER_FIELD_REFORMAT = SET_ORDER_FIELD_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_ORDER_FIELD_REFORMAT = collectMetrics(
        spark, 
        df_SET_ORDER_FIELD_REFORMAT, 
        "graph", 
        "Rtn6gdDnDHom8X82ECUQB$$qT3hOr-UUjkI4kR6Uyf74", 
        "SoD2EaDQ6MSh-zVXx3s16$$IqX9PAo4FOGUqTkRayc3a"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_ORDER_FIELD_REFORMAT)
    MD_MATL_VALUT_HIST(spark, df_SET_ORDER_FIELD_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "UKrgjurqM-TuI5MzSmCug$$DsloFtzh99z5_6JcEGR_C", 
        "mFUvKfp5yUvgp9hNbgEXm$$b-wOJBJxSYVNARgLb-hSS"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_VALUT_HIST")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_VALUT_HIST")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
