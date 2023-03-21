from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_src_sys.config.ConfigStore import *
from tbl_strct_src_sys.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_src_sys.graph import *

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_SRC_SYS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_SRC_SYS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
