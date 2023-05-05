from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_03_md_cust_hmd_hm2.config.ConfigStore import *
from sap_03_md_cust_hmd_hm2.udfs.UDFs import *
from prophecy.utils import *
from sap_03_md_cust_hmd_hm2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_TVAST = SAP_TVAST(spark)
    df_SAP_TVAST = collectMetrics(
        spark, 
        df_SAP_TVAST, 
        "graph", 
        "C62228oBz0jGVr_17sJC9$$8F-vQbhaubWfZWX32LcZt", 
        "4eWdB2mkUc8laZJCurVxT$$Ej_RE13XVe34BcdqmQ9o5"
    )
    df_SAP_T016T = SAP_T016T(spark)
    df_SAP_T016T = collectMetrics(
        spark, 
        df_SAP_T016T, 
        "graph", 
        "6GBasmueoVD5ee8TJdl7v$$1V4RjMs9af9qjy2JTPXsH", 
        "UUTQl86mWJ2bG_k9yMzTH$$dq3A_5BiBSePUEwlA9M-N"
    )
    df_MANDT_FILTER_TVAST = MANDT_FILTER_TVAST(spark, df_SAP_TVAST)
    df_MANDT_FILTER_T016T = MANDT_FILTER_T016T(spark, df_SAP_T016T)
    LU_SAP_T016T(spark, df_MANDT_FILTER_T016T)
    LU_SAP_TVAST(spark, df_MANDT_FILTER_TVAST)
    df_DS_SAP_03_KNA1 = DS_SAP_03_KNA1(spark)
    df_DS_SAP_03_KNA1 = collectMetrics(
        spark, 
        df_DS_SAP_03_KNA1, 
        "graph", 
        "XpOxQA5XbgQN-vhd8I3gh$$c7x9J4UPcRb9IO3owvBOr", 
        "qKr527nIzhOkZjHqJiLBb$$E1QQj9KbLNxqxV4gU5710"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_03_KNA1)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "bXWpFDjTHdH2yBxeeI2XA$$Ety3wHmWL8qDipzXNvxHW", 
        "FLq228VnghXj3TH2maDzc$$ckS75rklFFjfe1z8JCZmY"
    )
    MD_CUST(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_SHIP_SVS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_SHIP_SVS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
