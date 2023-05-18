from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_dstn_chn_bwi.config.ConfigStore import *
from sap_01_md_matl_dstn_chn_bwi.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_dstn_chn_bwi.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_t179t = SAP_t179t(spark)
    df_SAP_t179t = collectMetrics(
        spark, 
        df_SAP_t179t, 
        "graph", 
        "4lGPhcNh0VPhlNyxvz4uN$$lvqkl57I59o7UBLlgkn9s", 
        "zYOcFSRmhhfW94CrYd92L$$Dx0s1ZPxBkoHHcMVTpMTS"
    )
    df_MANDT_FILTER_t179t = MANDT_FILTER_t179t(spark, df_SAP_t179t)
    LU_SAP_t179t(spark, df_MANDT_FILTER_t179t)
    df_DS_SAP_01_MVKE = DS_SAP_01_MVKE(spark)
    df_DS_SAP_01_MVKE = collectMetrics(
        spark, 
        df_DS_SAP_01_MVKE, 
        "graph", 
        "d47JMjE5wgKv1mqAViKIo$$fu3Gx9WRBcmB52bsPdri5", 
        "UBwVO_QJdcVFzJwoZYTAb$$EbEjqySFvZtvz19p3lkkk"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_01_MVKE)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS)
    df_ORDER = ORDER(spark, df_DEDUPLICATE)
    df_ORDER = collectMetrics(
        spark, 
        df_ORDER, 
        "graph", 
        "PvGwDDwRJH0ORNLr0RmaI$$ghsrRUNnEMj3Dt_uRmkJq", 
        "oGbkzxtmzhGqiTKuJZhqu$$r_TP0K639NbXnfWm4lhOX"
    )
    MD_MATL_DSTN_CHN(spark, df_ORDER)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_md_matl_dstn_chn_bwi")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_md_matl_dstn_chn_bwi")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
