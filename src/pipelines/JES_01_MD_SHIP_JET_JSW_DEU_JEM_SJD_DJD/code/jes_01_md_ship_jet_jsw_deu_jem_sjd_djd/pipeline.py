from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jes_01_md_ship_jet_jsw_deu_jem_sjd_djd.config.ConfigStore import *
from jes_01_md_ship_jet_jsw_deu_jem_sjd_djd.udfs.UDFs import *
from prophecy.utils import *
from jes_01_md_ship_jet_jsw_deu_jem_sjd_djd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4211 = JDE_F4211(spark)
    df_JDE_F4211_FILTER = JDE_F4211_FILTER(spark, df_JDE_F4211)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_JDE_F4211_FILTER)
    df_SET_FIELD_ORDER_FORMAT = SET_FIELD_ORDER_FORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_JDE_F43121 = JDE_F43121(spark)
    df_JDE_F43121_FILTER = JDE_F43121_FILTER(spark, df_JDE_F43121)
    df_NEW_FIELDS_RENAME_FORMAT_2 = NEW_FIELDS_RENAME_FORMAT_2(spark, df_JDE_F43121_FILTER)
    df_SET_FIELD_ORDER_FORMAT_1 = SET_FIELD_ORDER_FORMAT_1(spark, df_NEW_FIELDS_RENAME_FORMAT_2)
    df_Deduplicate_1 = Deduplicate_1(spark, df_SET_FIELD_ORDER_FORMAT_1)
    df_UNION = UNION(spark, df_SET_FIELD_ORDER_FORMAT, df_Deduplicate_1)
    df_DUPLICATE = DUPLICATE(spark, df_UNION)
    MD_SHIP(spark, df_UNION)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JES_01_MD_SHIP_JET_JSW_DEU_JEM_SJD_DJD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JES_01_MD_SHIP_JET_JSW_DEU_JEM_SJD_DJD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
