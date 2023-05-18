from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4201 = JDE_F4201(spark)
    df_JDE_F4201 = collectMetrics(
        spark, 
        df_JDE_F4201, 
        "graph", 
        "oai-k6rsM3DaXL8zlH0gP$$4ma6w2BOf6zMU4wjWX7TZ", 
        "PXwL7I3W4R6S5dTxhzhwA$$GTHn8zlGc59X8m7Dg0wJi"
    )
    df_JDE_F4201_FILTER = JDE_F4201_FILTER(spark, df_JDE_F4201)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_JDE_F4201_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "fnuWA3iEP8yj0Wpmr7x7w$$RlB3rFyhHr8kKTVsFk6Jj", 
        "mtvb5N4jIUORTT5rEKyuI$$G_MoZoeS_MwazxHSJUA4l"
    )
    df_GET_DUP = GET_DUP(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUP_FILTER = DUP_FILTER(spark, df_GET_DUP)
    df_DUP_FILTER = collectMetrics(
        spark, 
        df_DUP_FILTER, 
        "graph", 
        "6Ox2NQn66uCJXu0tsLuTT$$OIhLh__wvgzXE0AXUcTi1", 
        "-ZvjtQMEZsEHoq2pY6-mY$$AC5SgPju82x-bO3RoNp85"
    )
    df_DUP_FILTER.cache().count()
    df_DUP_FILTER.unpersist()
    MD_SLS_ORDR(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set(
        "prophecy.metadata.pipeline.uri",
        "pipelines/JDE_01_MD_SLS_ORDR_BW2_GMD_JET_JSW_DEU_MTR_SJD_DJD_JEM_JES"
    )
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/JDE_01_MD_SLS_ORDR_BW2_GMD_JET_JSW_DEU_MTR_SJD_DJD_JEM_JES"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
