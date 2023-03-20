from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_bill_doc_hdr.config.ConfigStore import *
from sap_01_md_bill_doc_hdr.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_bill_doc_hdr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_VBRK = DS_SAP_01_VBRK(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_01_VBRK)
    df_Join_1 = Join_1(spark, df_MANDT_FILTER)

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
