from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ppln_sk_test.config.ConfigStore import *
from ppln_sk_test.udfs.UDFs import *
from prophecy.utils import *
from ppln_sk_test.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Script_1 = Script_1(spark)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Script_1)
    MD_MATL(spark, df_SchemaTransform_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/PPLN_SK_TEST")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/PPLN_SK_TEST")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
