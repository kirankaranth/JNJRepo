from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_mvmt_hdr_gmd_v2.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_gmd_v2.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_mvmt_hdr_gmd_v2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_F0005 = DS_JDE_F0005(spark)
    df_FILTER = FILTER(spark, df_DS_JDE_F0005)
    df_DS_JDE_F4111 = DS_JDE_F4111(spark)
    df_FILTER_DELETED = FILTER_DELETED(spark, df_DS_JDE_F4111)
    df_Join_1 = Join_1(spark, df_FILTER_DELETED, df_FILTER)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELDS_OUTPUT = SET_FIELDS_OUTPUT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    MD_MATL_MVMT_HDR(spark, df_SET_FIELDS_OUTPUT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL_MVMT_HDR_GMD_v2")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MATL_MVMT_HDR_GMD_v2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
