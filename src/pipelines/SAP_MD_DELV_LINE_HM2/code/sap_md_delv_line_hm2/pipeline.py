from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_line_hm2.config.ConfigStore import *
from sap_md_delv_line_hm2.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_line_hm2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_LIKP = DS_SAP_01_LIKP(spark)
    df_DS_SAP_01_LIPS = DS_SAP_01_LIPS(spark)
    df_DS_SAP_01_TVM4T = DS_SAP_01_TVM4T(spark)
    df_DS_SAP_01_VBAP = DS_SAP_01_VBAP(spark)
    df_DS_SAP_01_VBAK = DS_SAP_01_VBAK(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_DELV_LINE_HM2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_DELV_LINE_HM2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
