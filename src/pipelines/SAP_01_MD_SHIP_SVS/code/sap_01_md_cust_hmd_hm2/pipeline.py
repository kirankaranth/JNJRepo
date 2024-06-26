from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_cust_hmd_hm2.config.ConfigStore import *
from sap_01_md_cust_hmd_hm2.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_cust_hmd_hm2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_T077X = SAP_T077X(spark)
    df_SAP_T077X = collectMetrics(
        spark, 
        df_SAP_T077X, 
        "graph", 
        "KpZ3e4RB1O7lbbls6RYf6$$Atl0h1-c9MlYelA5GGEXu", 
        "HzJOPpwgLPsF7hktqVMj5$$XTuJ_dAKZt1DReK-MI3Nw"
    )
    df_MANDT_FILTER_T077X = MANDT_FILTER_T077X(spark, df_SAP_T077X)
    LU_SAP_T077X(spark, df_MANDT_FILTER_T077X)
    df_SAP_T016T = SAP_T016T(spark)
    df_SAP_T016T = collectMetrics(
        spark, 
        df_SAP_T016T, 
        "graph", 
        "8cFL-3uwRdK7UKS437ZS3$$YABPAokV6CdXobHKP3N4N", 
        "r0kVN8MKkll1Qq7yCvrZH$$XGvdY2Y9Wxkg85kGi6Yxr"
    )
    df_MANDT_FILTER_T016T = MANDT_FILTER_T016T(spark, df_SAP_T016T)
    df_SAP_TBRCT = SAP_TBRCT(spark)
    df_SAP_TBRCT = collectMetrics(
        spark, 
        df_SAP_TBRCT, 
        "graph", 
        "-ZYPpyu0zwycNH4wusoJQ$$oWNVTg6o3namfmChvdUMa", 
        "qMKjRdlVr7zs1BhDJ_T5B$$muDk5IGGHEuObXaYY3EFW"
    )
    df_SAP_TVAST = SAP_TVAST(spark)
    df_SAP_TVAST = collectMetrics(
        spark, 
        df_SAP_TVAST, 
        "graph", 
        "C62228oBz0jGVr_17sJC9$$8F-vQbhaubWfZWX32LcZt", 
        "4eWdB2mkUc8laZJCurVxT$$Ej_RE13XVe34BcdqmQ9o5"
    )
    df_MANDT_FILTER_TVAST = MANDT_FILTER_TVAST(spark, df_SAP_TVAST)
    LU_SAP_TVAST(spark, df_MANDT_FILTER_TVAST)
    LU_SAP_03_T077X(spark, df_MANDT_FILTER_T077X)
    df_MANDT_FILTER_TBRCT = MANDT_FILTER_TBRCT(spark, df_SAP_TBRCT)
    LU_SAP_TBRCT(spark, df_MANDT_FILTER_TBRCT)
    LU_SAP_02_T077X(spark, df_MANDT_FILTER_T077X)
    LU_SAP_T016T(spark, df_MANDT_FILTER_T016T)
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
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "bXWpFDjTHdH2yBxeeI2XA$$Ety3wHmWL8qDipzXNvxHW", 
        "FLq228VnghXj3TH2maDzc$$ckS75rklFFjfe1z8JCZmY"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "Imd7XS6t2L_zT1fY-XbFV$$oe6XwBp-yvxwhFE9xOgj0", 
        "Tj9XT4hJN94v-DLqCrPZL$$6L1HyQzDixojFSt5O6xop"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
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
