from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_sls_ordr_line_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr.config.ConfigStore import *
from jde_md_sls_ordr_line_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr.udfs.UDFs import *
from prophecy.utils import *
from jde_md_sls_ordr_line_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4211 = JDE_F4211(spark)
    df_DELETED_FILTER = DELETED_FILTER(spark, df_JDE_F4211)
    df_f4211 = f4211(spark, df_DELETED_FILTER)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_f4211)
    df_JDE_F42119 = JDE_F42119(spark)
    df_DELETED_FILTER_1 = DELETED_FILTER_1(spark, df_JDE_F42119)
    df_f42119 = f42119(spark, df_DELETED_FILTER_1)
    df_NEW_FIELDS_RENAME_FORMAT_1 = NEW_FIELDS_RENAME_FORMAT_1(spark, df_f42119)
    df_UNION = UNION(spark, df_NEW_FIELDS_RENAME_FORMAT, df_NEW_FIELDS_RENAME_FORMAT_1)
    df_SET_FIELDS_REFORMAT_ORDER = SET_FIELDS_REFORMAT_ORDER(spark, df_UNION)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_SET_FIELDS_REFORMAT_ORDER)
    MD_SLS_ORDR_LINE(spark, df_DEDUPLICATE)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_SLS_ORDR_LINE_")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_SLS_ORDR_LINE_")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
