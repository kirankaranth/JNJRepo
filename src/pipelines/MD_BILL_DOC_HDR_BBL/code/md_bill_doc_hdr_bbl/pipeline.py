from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_bill_doc_hdr_bbl.config.ConfigStore import *
from md_bill_doc_hdr_bbl.udfs.UDFs import *
from prophecy.utils import *
from md_bill_doc_hdr_bbl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_TVKOT = SAP_TVKOT(spark)
    df_MANDT_FILTER_TVKOT = MANDT_FILTER_TVKOT(spark, df_SAP_TVKOT)
    LU_SAP_TVKOT(spark, df_MANDT_FILTER_TVKOT)
    df_SAP_TVFKT = SAP_TVFKT(spark)
    df_SAP_TVTWT = SAP_TVTWT(spark)
    df_MANDT_FILTER_TVTWT = MANDT_FILTER_TVTWT(spark, df_SAP_TVTWT)
    df_MANDT_FILTER_TVFKT = MANDT_FILTER_TVFKT(spark, df_SAP_TVFKT)
    LU_SAP_TVFKT(spark, df_MANDT_FILTER_TVFKT)
    df_SAP_TSPAT = SAP_TSPAT(spark)
    df_MANDT_FILTER_TSPAT = MANDT_FILTER_TSPAT(spark, df_SAP_TSPAT)
    LU_SAP_TSPAT(spark, df_MANDT_FILTER_TSPAT)
    LU_SAP_TVTWT(spark, df_MANDT_FILTER_TVTWT)
    df_SAP_VBRK = SAP_VBRK(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VBRK)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_FORMAT = SET_FIELD_ORDER_FORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    MD_BILL_DOC_HDR(spark, df_SET_FIELD_ORDER_FORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_FORMAT)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_BILL_DOC_HDR_BBL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_BILL_DOC_HDR_BBL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
