from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_crncy_svs_bwi_atl.config.ConfigStore import *
from sap_md_crncy_svs_bwi_atl.udfs.UDFs import *
from prophecy.utils import *
from sap_md_crncy_svs_bwi_atl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_TCURC = DS_SAP_01_TCURC(spark)
    df_DS_SAP_01_TCURC = collectMetrics(
        spark, 
        df_DS_SAP_01_TCURC, 
        "graph", 
        "Dhn1Pn0Y__nrUilYSbMhS$$XboEe6CiG6z8boV-iBDRb", 
        "pt517oGDlYXEIWxfUUc1K$$qoxHooId6YTkIZjSuvlyk"
    )
    df_MANDT_FILTER_TCURC = MANDT_FILTER_TCURC(spark, df_DS_SAP_01_TCURC)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER_TCURC)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "WskbXQvifN7Bu_BF0LTKY$$nV_mZKyRy4XSGXvHOP7IK", 
        "3lk_UxSPJFljKQAD-CXdZ$$meaqYDsjeiPewBuAZ-89d"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "JK9CE5M-51iw352R2vuPx$$MkEEuOKzGqxhp_S_K4OUC", 
        "y5kyo5Fllk0oVajzvuaKG$$Md7ZAnVpYCoQQZCQ7H6XY"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
    MD_CRNCY(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CRNCY_SVS_BWI_ATL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CRNCY_SVS_BWI_ATL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
