from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_tai.config.ConfigStore import *
from sap_md_co_cd_tai.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_TAI_KNB1 = DS_SAP_TAI_KNB1(spark)
    df_DS_SAP_TAI_KNB1 = collectMetrics(
        spark, 
        df_DS_SAP_TAI_KNB1, 
        "graph", 
        "B_rV4SL3o4C9X5Bzr9r2M$$mbEi_tYeGgrQKP7erC5sE", 
        "lgjfHOowZKVSPhhmgoJzF$$oQ3wzk0TzLaOoTe2PgY_f"
    )
    df_MANDT_FILTER_KNB1 = MANDT_FILTER_KNB1(spark, df_DS_SAP_TAI_KNB1)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER_KNB1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "Vm2pGXm1Bt6FbpRXyrLfS$$w7OhUGHN9la8lmAwYWi5a", 
        "womC2ySuX8DtfXsBkZU6l$$SX9YVj-4PCgmo3KWV20lG"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    MD_CO_CD(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "bUzrif-aNCccLLZz5eYyF$$sDeOzJ9Tafj0eL2CVabfO", 
        "44lkVS6RGE_CViptfnBKK$$Mi8yIw-iAzZGSIoheiNHh"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_TAI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_TAI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
