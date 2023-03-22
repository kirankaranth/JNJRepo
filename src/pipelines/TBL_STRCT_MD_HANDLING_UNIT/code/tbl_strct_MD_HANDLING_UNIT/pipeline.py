from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_HANDLING_UNIT.config.ConfigStore import *
from tbl_strct_MD_HANDLING_UNIT.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_HANDLING_UNIT.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_HU = sql_MD_HU(spark)
    MD_HU(spark, df_sql_MD_HU)
    df_sql_MD_HU_LINE = sql_MD_HU_LINE(spark)
    df_sql_MD_HU_SER = sql_MD_HU_SER(spark)
    MD_HU_SER(spark, df_sql_MD_HU_SER)
    MD_HU_LINE(spark, df_sql_MD_HU_LINE)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_HANDLING_UNIT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_HANDLING_UNIT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
