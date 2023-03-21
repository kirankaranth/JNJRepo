from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_matl_inv.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4101 = JDE_F4101(spark)
    df_JDE_F41021_4 = JDE_F41021_4(spark)
    df_JDE_F4101_3 = JDE_F4101_3(spark)
    df_JDE_F41021 = JDE_F41021(spark)
    df_LOT_STATUS_QOH = LOT_STATUS_QOH(spark, df_JDE_F41021)
    df_Join_1 = Join_1(spark, df_JDE_F4101, df_LOT_STATUS_QOH)
    MD_MAT_INV_SWAP(spark, df_Join_1)
    df_JDE_F4101_4 = JDE_F4101_4(spark)
    df_JDE_F4101_2 = JDE_F4101_2(spark)
    df_JDE_F4101_5 = JDE_F4101_5(spark)
    df_JDE_F41021_3 = JDE_F41021_3(spark)
    df_JDE_F41021_2 = JDE_F41021_2(spark)
    df_JDE_F4101_6 = JDE_F4101_6(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_MATL_INV")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_MATL_INV")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
