from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_bbl.config.ConfigStore import *
from sap_md_co_cd_bbl.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_bbl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_HMD_KNB1 = DS_SAP_HMD_KNB1(spark)
    df_DS_SAP_HMD_KNB1 = collectMetrics(
        spark, 
        df_DS_SAP_HMD_KNB1, 
        "graph", 
        "OBCNwkwCpgj8O9zXum9M9$$wHB86NCPE1288awG1QMfT", 
        "uhnYnpkBydpg3p7Y5FM7U$$uTJRQk-oDMOEZfbSTpVPR"
    )
    df_MANDT_FILTER_KNB1 = MANDT_FILTER_KNB1(spark, df_DS_SAP_HMD_KNB1)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER_KNB1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "0_FAgDhic5Ru5JqsiOCH1$$Z4GJbfD_P6JD5pPrEoT1V", 
        "nrvcqHv1zh06sox6BlCMj$$3-HkXDU7sSZUBapxL93e7"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    MD_CO_CD(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "JhhYJzQiwpLa64q13S7NJ$$BPtpyNYp-TWmpHmvMIzYq", 
        "WNraZ5ctDikhlWQ25ipaO$$8pxbJsAW7P4YyPnVfmJDV"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_BBL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_BBL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
