from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.udfs.UDFs import *
from prophecy.utils import *
from md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_MAKT = MAKT(spark)
    df_DEL_AND_MANDT_1 = DEL_AND_MANDT_1(spark, df_MAKT)
    MAKTX_LU(spark, df_DEL_AND_MANDT_1)
    df_MARA_MBP = MARA_MBP(spark)
    df_DEL_AND_MANDT = DEL_AND_MANDT(spark, df_MARA_MBP)
    df_XFORM = XFORM(spark, df_DEL_AND_MANDT)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    TARGET(spark, df_SELECT_FIELDS)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_MBP")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_MBP")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
