from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_sls_ordr_line.config.ConfigStore import *
from sap_01_md_sls_ordr_line.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_sls_ordr_line.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_TVM2T = SAP_TVM2T(spark)
    df_SAP_VBAP = SAP_VBAP(spark)
    df_SAP_VBKD = SAP_VBKD(spark)
    df_SAP_TVAPT = SAP_TVAPT(spark)
    df_SAP_TVM1T = SAP_TVM1T(spark)
    df_SAP_TVAGT = SAP_TVAGT(spark)
    df_SAP_VBAK = SAP_VBAK(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_SLS_ORDR_LINE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_SLS_ORDR_LINE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
