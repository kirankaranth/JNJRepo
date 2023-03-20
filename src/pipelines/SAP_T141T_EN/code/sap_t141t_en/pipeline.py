from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_t141t_en.config.ConfigStore import *
from sap_t141t_en.udfs.UDFs import *
from prophecy.utils import *
from sap_t141t_en.graph import *

def pipeline(spark: SparkSession) -> None:
    SAP_T141T_Lookup(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_T141T_EN")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_T141T_EN")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
