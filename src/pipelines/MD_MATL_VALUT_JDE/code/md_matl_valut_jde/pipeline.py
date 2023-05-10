from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *
from prophecy.utils import *
from md_matl_valut_jde.graph import *

def pipeline(spark: SparkSession) -> None:
    df_F4101 = F4101(spark)
    df_SELECT = SELECT(spark, df_F4101)
    df_DEL1 = DEL1(spark, df_SELECT)
    LITM_LU(spark, df_DEL1)
    UOM_GLPT(spark, df_DEL1)
    df_F41021 = F41021(spark)
    df_DEL = DEL(spark, df_F41021)
    df_INV_SUM = INV_SUM(spark, df_DEL)
    df_F4105 = F4105(spark)
    df_COLEDG = COLEDG(spark, df_F4105)
    TEST(spark, df_INV_SUM)

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
