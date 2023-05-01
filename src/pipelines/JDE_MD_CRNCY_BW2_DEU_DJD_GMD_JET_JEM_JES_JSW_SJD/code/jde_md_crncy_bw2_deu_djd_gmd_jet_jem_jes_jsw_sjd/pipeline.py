from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.config.ConfigStore import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F0013 = DS_JDE_01_F0013(spark)
    df_DELETED_FILTER_F0013 = DELETED_FILTER_F0013(spark, df_DS_JDE_01_F0013)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_DELETED_FILTER_F0013)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_CRNCY_BW2_DEU_DJD_GMD_JET_JEM_JES_JSW_SJD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_CRNCY_BW2_DEU_DJD_GMD_JET_JEM_JES_JSW_SJD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
