from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sk_sap_01_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sk_sap_01_md_ser_num_stock_sgmnt.udfs.UDFs import *
from prophecy.utils import *
from sk_sap_01_md_ser_num_stock_sgmnt.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_EQBS = SAP_EQBS(spark)
    df_MANDT_Filter_Parameter = MANDT_Filter_Parameter(spark, df_SAP_EQBS)
    df_SET_FIELD_ORDER_REFORMAT_1 = SET_FIELD_ORDER_REFORMAT_1(spark, df_MANDT_Filter_Parameter)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_SET_FIELD_ORDER_REFORMAT_1)
    MD_SER_NUM_STOCK_SGMNT(spark, df_NEW_FIELDS_RENAME_FORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SK_SAP_01_MD_SER_NUM_STOCK_SGMNT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SK_SAP_01_MD_SER_NUM_STOCK_SGMNT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()