from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_dstn_chn.config.ConfigStore import *
from sap_01_md_matl_dstn_chn.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_dstn_chn.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_MVKE = DS_SAP_01_MVKE(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_01_MVKE)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_ORDER = ORDER(spark, df_NEW_FIELDS)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_ORDER)
    MD_MATL_DSTN_CHN(spark, df_DEDUPLICATE)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_md_matl_dstn_chn")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_md_matl_dstn_chn")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
