from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_cust_mstr_unld_data.config.ConfigStore import *
from sap_md_cust_mstr_unld_data.udfs.UDFs import *
from prophecy.utils import *
from sap_md_cust_mstr_unld_data.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_KNVA = DS_SAP_01_KNVA(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_01_KNVA)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_Reformat_1 = Reformat_1(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_Aggregate_1 = Aggregate_1(spark, df_Reformat_1)
    df_Filter_1 = Filter_1(spark, df_Aggregate_1)
    MD_CUST_MSTR_UNLD_DATA(spark, df_Reformat_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CUST_MSTR_UNLD_DATA")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CUST_MSTR_UNLD_DATA")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
