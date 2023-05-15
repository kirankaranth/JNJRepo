from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_bba_bbn_hcs_mrs_bwi.config.ConfigStore import *
from sap_md_co_cd_bba_bbn_hcs_mrs_bwi.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_bba_bbn_hcs_mrs_bwi.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_BBA_BBN_HCS_MRS_BWI_KNB1 = DS_SAP_BBA_BBN_HCS_MRS_BWI_KNB1(spark)
    df_DS_SAP_BBA_BBN_HCS_MRS_BWI_KNB1 = collectMetrics(
        spark, 
        df_DS_SAP_BBA_BBN_HCS_MRS_BWI_KNB1, 
        "graph", 
        "HujGhNMYGOAB8aTu_G5OZ$$AAH-d98gcLHd2wZu8l9Jc", 
        "DdlkhrhyKUhFJAeH5GILw$$cH7Tb8Y6t3M4p8T5irdFH"
    )
    df_MANDT_FILTER_KNB1 = MANDT_FILTER_KNB1(spark, df_DS_SAP_BBA_BBN_HCS_MRS_BWI_KNB1)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER_KNB1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "rpO9pxK20a71tqPqjk6xU$$Crq3I2mCiYTc-Kvy6BYMZ", 
        "l6l643X44GmmNJrsGYcCm$$NcVYTLcZ-fzaqlqjYw97v"
    )
    MD_CO_CD(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "7LTQXypqayX6MKMZYtjoc$$PWz7xwI-WdQzIK2i78xs7", 
        "fqlMjlhAM5uV3OYvl2eWB$$ctQOO3MPrOq7OalF41mPy"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_BBA_BBN_HCS_MRS_BWI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_BBA_BBN_HCS_MRS_BWI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
