from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_mfg_order_itm.config.ConfigStore import *
from md_mfg_order_itm.udfs.UDFs import *
from prophecy.utils import *
from md_mfg_order_itm.graph import *

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_MFG_ORDER_ITM")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_MFG_ORDER_ITM")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
