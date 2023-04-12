from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *
from prophecy.utils import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F43121 = DS_JDE_01_F43121(spark)
    df_DELETED_FILTER_F43121 = DELETED_FILTER_F43121(spark, df_DS_JDE_01_F43121)
    df_NEW_FIELDS_RENAME_FORMAT_F43121 = NEW_FIELDS_RENAME_FORMAT_F43121(spark, df_DELETED_FILTER_F43121)
    df_SET_FIELD_ORDER_REFORMAT_F43121 = SET_FIELD_ORDER_REFORMAT_F43121(spark, df_NEW_FIELDS_RENAME_FORMAT_F43121)
    df_DS_JDE_01_F4211 = DS_JDE_01_F4211(spark)
    df_DELETED_FILTER_F4211 = DELETED_FILTER_F4211(spark, df_DS_JDE_01_F4211)
    df_NEW_FIELDS_RENAME_FORMAT_F4211 = NEW_FIELDS_RENAME_FORMAT_F4211(spark, df_DELETED_FILTER_F4211)
    df_SET_FIELD_ORDER_REFORMAT_F4211 = SET_FIELD_ORDER_REFORMAT_F4211(spark, df_NEW_FIELDS_RENAME_FORMAT_F4211)
    df_UNION = UNION(spark, df_SET_FIELD_ORDER_REFORMAT_F43121, df_SET_FIELD_ORDER_REFORMAT_F4211)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_UNION)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_DEDUPLICATE)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    JDE_MD_DELV_LINE(spark, df_DEDUPLICATE)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_DELV_LINE_BW2_GMD_JET_JSW_DEU_MTR_SJD_DJD_JEM_JES")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/JDE_MD_DELV_LINE_BW2_GMD_JET_JSW_DEU_MTR_SJD_DJD_JEM_JES"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
