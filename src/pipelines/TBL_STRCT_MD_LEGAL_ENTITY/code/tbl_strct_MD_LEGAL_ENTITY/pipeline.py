from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_LEGAL_ENTITY.config.ConfigStore import *
from tbl_strct_MD_LEGAL_ENTITY.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_LEGAL_ENTITY.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_LE = sql_MD_LE(spark)
    MD_LE(spark, df_sql_MD_LE)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_LEGAL_ENTITY")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_LEGAL_ENTITY")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
