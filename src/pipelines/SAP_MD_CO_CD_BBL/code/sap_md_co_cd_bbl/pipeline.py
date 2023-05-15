from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_bbl.config.ConfigStore import *
from sap_md_co_cd_bbl.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_bbl.graph import *

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
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_BBL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_BBL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
