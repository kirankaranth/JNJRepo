from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_mvmt_hdr_hmd.config.ConfigStore import *
from sap_md_matl_mvmt_hdr_hmd.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_mvmt_hdr_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_MATDOC = DS_SAP_MATDOC(spark)
    df_DS_SAP_T003T = DS_SAP_T003T(spark)
    df_FILTER_MANDT_1 = FILTER_MANDT_1(spark, df_DS_SAP_T003T)
    df_FILTER_MANDT = FILTER_MANDT(spark, df_DS_SAP_MATDOC)
    df_Join_1 = Join_1(spark, df_FILTER_MANDT, df_FILTER_MANDT_1)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_MVMT_HDR_HMD")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_MVMT_HDR_HMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
