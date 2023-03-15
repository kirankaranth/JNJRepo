from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_vendormastermd_sup_prchsng_org.config.ConfigStore import *
from sap_01_md_vendormastermd_sup_prchsng_org.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_vendormastermd_sup_prchsng_org.graph import *

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/sap_01_md_vendor-master-md_sup_prchsng_org")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/sap_01_md_vendor-master-md_sup_prchsng_org")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
