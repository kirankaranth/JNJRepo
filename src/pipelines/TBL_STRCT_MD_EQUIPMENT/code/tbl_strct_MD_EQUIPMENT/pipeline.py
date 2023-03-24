from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_EQUIPMENT.config.ConfigStore import *
from tbl_strct_MD_EQUIPMENT.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_EQUIPMENT.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_EQMNT = sql_MD_EQMNT(spark)
    MD_EQMNT(spark, df_sql_MD_EQMNT)
    df_sql_MD_EQMNT_BOM_LINK = sql_MD_EQMNT_BOM_LINK(spark)
    MD_EQMNT_BOM_LINK(spark, df_sql_MD_EQMNT_BOM_LINK)
    df_sql_MD_SER_NUM_STOCK_SGMNT = sql_MD_SER_NUM_STOCK_SGMNT(spark)
    MD_SER_NUM_STOCK_SGMNT(spark, df_sql_MD_SER_NUM_STOCK_SGMNT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_EQUIPMENT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_EQUIPMENT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
