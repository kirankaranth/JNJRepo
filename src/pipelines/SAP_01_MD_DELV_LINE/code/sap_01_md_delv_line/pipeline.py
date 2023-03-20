from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_delv_line.config.ConfigStore import *
from sap_01_md_delv_line.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_delv_line.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_AFVC = DS_SAP_01_AFVC(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_DELV_LINE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_DELV_LINE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
