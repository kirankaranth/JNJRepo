from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ngem_material_cross_reference.config.ConfigStore import *
from ngem_material_cross_reference.udfs.UDFs import *
from prophecy.utils import *
from ngem_material_cross_reference.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_NGEM_MATERIAL_CROSS_REFERENCE = DS_NGEM_MATERIAL_CROSS_REFERENCE(spark)
    df_Source_1 = Source_1(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/NGEM_MATERIAL_CROSS_REFERENCE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/NGEM_MATERIAL_CROSS_REFERENCE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
