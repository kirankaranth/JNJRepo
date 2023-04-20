from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tbl_strct_MD_BILL_OF_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_BILL_OF_MATERIAL.udfs.UDFs import *
from prophecy.utils import *
from tbl_strct_MD_BILL_OF_MATERIAL.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_BOM_HDR = sql_MD_BOM_HDR(spark)
    MD_BOM_HDR(spark, df_sql_MD_BOM_HDR)
    df_sql_MD_BOM_ITM_NODE = sql_MD_BOM_ITM_NODE(spark)
    MD_BOM_ITM_NODE(spark, df_sql_MD_BOM_ITM_NODE)
    df_sql_MD_BOM_ITM = sql_MD_BOM_ITM(spark)
    MD_BOM_ITM(spark, df_sql_MD_BOM_ITM)
    df_sql_MD_MATL_BOM = sql_MD_MATL_BOM(spark)
    MD_MATL_BOM(spark, df_sql_MD_MATL_BOM)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/TBL_STRCT_MD_BILL_OF_MATERIAL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/TBL_STRCT_MD_BILL_OF_MATERIAL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
