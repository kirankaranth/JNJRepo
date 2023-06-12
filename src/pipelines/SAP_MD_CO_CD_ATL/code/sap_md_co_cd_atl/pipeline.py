from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_atl.config.ConfigStore import *
from sap_md_co_cd_atl.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_atl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_ATL_KNB1 = DS_SAP_ATL_KNB1(spark)
    df_DS_SAP_ATL_KNB1 = collectMetrics(
        spark, 
        df_DS_SAP_ATL_KNB1, 
        "graph", 
        "TfcBDH4H_pFGwNOt-TXhI$$stnojPUsW_BpW5tR9Nq_t", 
        "HGTfmn1WJUejwm3RWFCxT$$jvrVr7rell4ybH8LhJ3Hm"
    )
    df_MANDT_FILTER_KNB1 = MANDT_FILTER_KNB1(spark, df_DS_SAP_ATL_KNB1)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER_KNB1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "DLAdyDZVIkwnsrLkfcYrj$$v4sWuNO1WlHgYPqESbb0I", 
        "YNQgPxyUWxkL5Ctdgw7YD$$ztJweOqLXUOAxlTbxltnE"
    )
    MD_CO_CD(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "7GqJDoTJ_JggK5DIRkFlE$$YSKpJ-RXqkVnvnNPnSyzo", 
        "BG6vIYcC7GL_bmtEbBW0e$$SyueW3-vUDsX2mCjyjIKT"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_ATL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_ATL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
