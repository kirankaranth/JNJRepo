from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_mfg_order.config.ConfigStore import *
from jde_md_mfg_order.udfs.UDFs import *
from prophecy.utils import *
from jde_md_mfg_order.graph import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MFG_ORDER")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MFG_ORDER")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
