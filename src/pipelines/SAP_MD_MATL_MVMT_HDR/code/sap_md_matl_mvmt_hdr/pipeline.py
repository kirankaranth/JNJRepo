from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_mvmt_hdr.config.ConfigStore import *
from sap_md_matl_mvmt_hdr.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_mvmt_hdr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_MKPF = DS_SAP_MKPF(spark)
    df_FILTER_MANDT = FILTER_MANDT(spark, df_DS_SAP_MKPF)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_FILTER_MANDT)
    df_SET_FIELDS_OUTPUT = SET_FIELDS_OUTPUT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    MD_MATL_MVMT_HDR(spark, df_SET_FIELDS_OUTPUT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_MVMT_HDR")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_MVMT_HDR")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
