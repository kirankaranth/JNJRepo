from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *
from prophecy.utils import *
from md_matl_valut_jde.graph import *

def pipeline(spark: SparkSession) -> None:
    df_F4105 = F4105(spark)
    df_COLEDG_COCSIN = COLEDG_COCSIN(spark, df_F4105)
    df_DE_DUP_COST_AVG = DE_DUP_COST_AVG(spark, df_COLEDG_COCSIN)
    F4105_LU(spark, df_DE_DUP_COST_AVG)
    df_F4101 = F4101(spark)
    df_SELECT = SELECT(spark, df_F4101)
    df_DEL1 = DEL1(spark, df_SELECT)
    F4101_LU(spark, df_DEL1)
    df_F41021 = F41021(spark)
    df_DEL = DEL(spark, df_F41021)
    df_INV_SUM = INV_SUM(spark, df_DEL)
    df_XFORM = XFORM(spark, df_INV_SUM)
    df_SQLStatement_1 = SQLStatement_1(spark, df_XFORM)
    TEST(spark, df_SQLStatement_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_MATL_VALUT_JDE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_MATL_VALUT_JDE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
