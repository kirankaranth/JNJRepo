from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_bill_doc_hdr.config.ConfigStore import *
from sap_01_md_bill_doc_hdr.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_bill_doc_hdr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_VBRK = SAP_VBRK(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VBRK)
    df_SAP_TVKOT = SAP_TVKOT(spark)
    df_MANDT_FILTER_TVKOT = MANDT_FILTER_TVKOT(spark, df_SAP_TVKOT)
    df_SAP_TVTWT = SAP_TVTWT(spark)
    df_MANDT_FILTER_TVTWT = MANDT_FILTER_TVTWT(spark, df_SAP_TVTWT)
    df_SAP_TSPAT = SAP_TSPAT(spark)
    df_MANDT_FILTER_TSPAT = MANDT_FILTER_TSPAT(spark, df_SAP_TSPAT)
    df_SAP_TVFKT = SAP_TVFKT(spark)
    df_MANDT_FILTER_TVFKT = MANDT_FILTER_TVFKT(spark, df_SAP_TVFKT)
    df_Join_1 = Join_1(
        spark, 
        df_MANDT_FILTER, 
        df_MANDT_FILTER_TVKOT, 
        df_MANDT_FILTER_TVTWT, 
        df_MANDT_FILTER_TSPAT, 
        df_MANDT_FILTER_TVFKT
    )
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELD_ORDER_FORMAT = SET_FIELD_ORDER_FORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_FORMAT)
    MD_BILL_DOC_HDR(spark, df_SET_FIELD_ORDER_FORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_BILL_DOC_HDR")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_BILL_DOC_HDR")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
