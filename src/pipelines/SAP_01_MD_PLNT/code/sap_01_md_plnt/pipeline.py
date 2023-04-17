from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_plnt.config.ConfigStore import *
from sap_01_md_plnt.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_plnt.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_T005T = SAP_T005T(spark)
    df_MANDT_FILTER_T005T = MANDT_FILTER_T005T(spark, df_SAP_T005T)
    LU_SAP_T005T(spark, df_MANDT_FILTER_T005T)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_PLNT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_PLNT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
