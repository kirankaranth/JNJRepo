from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_hmd.config.ConfigStore import *
from sap_md_co_cd_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_HMD_KNB1 = DS_SAP_HMD_KNB1(spark)
    df_DS_SAP_HMD_KNB1 = collectMetrics(
        spark, 
        df_DS_SAP_HMD_KNB1, 
        "graph", 
        "4jmjbcEgjvH-pCVBPu3zK$$ic9E5LWXkeYJpUTr5SX8g", 
        "ludZqzQwhxF8rEpXfN9zX$$BBI6NWCN6SUMlnrlrf74I"
    )
    df_MANDT_FILTER_KNB1 = MANDT_FILTER_KNB1(spark, df_DS_SAP_HMD_KNB1)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER_KNB1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "YEPLZ1gkhdhSDWe84cDWA$$waB8taoBa_MP0zyeov521", 
        "daIHWQon9KMFrJE5NLrUM$$VZ9MGNUuyO0Vtq23SBGfy"
    )
    MD_CO_CD(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "dMYC0hCvnmgHelL077sZq$$EpXTmnOJN7CZsMF1liA6u", 
        "HZcK1USXnzOrN2xtNqasy$$KfLvzWMvsv6Y9GopmFw5e"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_HMD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_HMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
