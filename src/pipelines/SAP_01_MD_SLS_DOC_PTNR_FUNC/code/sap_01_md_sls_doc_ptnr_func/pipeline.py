from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_sls_doc_ptnr_func.config.ConfigStore import *
from sap_01_md_sls_doc_ptnr_func.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_sls_doc_ptnr_func.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_VBPA = SAP_VBPA(spark)
    df_SAP_VBAK = SAP_VBAK(spark)
    df_MANDT_VBPA = MANDT_VBPA(spark, df_SAP_VBPA)
    df_MANDT_VBAK = MANDT_VBAK(spark, df_SAP_VBAK)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_SLS_DOC_PTNR_FUNC")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_SLS_DOC_PTNR_FUNC")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
