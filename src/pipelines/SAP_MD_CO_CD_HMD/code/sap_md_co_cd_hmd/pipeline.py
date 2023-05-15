from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_hmd.config.ConfigStore import *
from sap_md_co_cd_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_HMD_KNB1 = DS_SAP_HMD_KNB1(spark)
    df_MANDT_FILTER_KNB1 = MANDT_FILTER_KNB1(spark, df_DS_SAP_HMD_KNB1)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER_KNB1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_HMD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_HMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
