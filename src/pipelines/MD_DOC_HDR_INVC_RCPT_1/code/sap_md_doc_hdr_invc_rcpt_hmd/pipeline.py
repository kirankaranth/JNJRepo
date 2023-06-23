from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_doc_hdr_invc_rcpt_hmd.config.ConfigStore import *
from sap_md_doc_hdr_invc_rcpt_hmd.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_doc_hdr_invc_rcpt_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DOC_HDR_INVC_RCPT = sql_MD_DOC_HDR_INVC_RCPT(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_DOC_HDR_INVC_RCPT)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "603b2390-6d4d-4c8b-ac43-b84223847077", 
        "820bbef7-1cfd-47ea-b719-107bf6e06475"
    )
    MD_DOC_HDR_INVC_RCPT(spark, df_addL1fields)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_DOC_HDR_INVC_RCPT_1")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_DOC_HDR_INVC_RCPT_1")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
