from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_p01_geu_svs.config.ConfigStore import *
from sap_md_co_cd_p01_geu_svs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_p01_geu_svs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_P01_BBL_GEU_SVS_KNB1 = DS_SAP_P01_BBL_GEU_SVS_KNB1(spark)
    df_DS_SAP_P01_BBL_GEU_SVS_KNB1 = collectMetrics(
        spark, 
        df_DS_SAP_P01_BBL_GEU_SVS_KNB1, 
        "graph", 
        "pv27B2is8mosmYKzg1oGD$$_PRB7JyFTvyBMYY072rYy", 
        "4-_Kw3i06vHadwGblAZTZ$$mFcN0COvytLGMTwq1W6B2"
    )
    df_MANDT_FILTER_KNB1 = MANDT_FILTER_KNB1(spark, df_DS_SAP_P01_BBL_GEU_SVS_KNB1)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER_KNB1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "1duyZt9cxreBcOF0Ut-hz$$FoThldPPbLeJIoAMPg-vm", 
        "Su_IEU4j-UFf-GEWvKpGL$$Xqh2i-6QTkn1t_H6-ZNCq"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "9C59_9uFpdIa5oTw0jW50$$vF7cx98zfEI0g8MMRV310", 
        "pWJpw9KufqJMVaeDd2L_G$$tQ0dqZiOPHxWYzhGWzRlo"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
    MD_CO_CD(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_P01_BBL_GEU_SVS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_P01_BBL_GEU_SVS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
