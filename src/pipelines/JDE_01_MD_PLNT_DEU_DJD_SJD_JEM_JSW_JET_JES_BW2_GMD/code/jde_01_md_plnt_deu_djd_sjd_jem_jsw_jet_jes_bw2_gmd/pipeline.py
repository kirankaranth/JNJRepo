from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.config.ConfigStore import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F0006 = JDE_F0006(spark)
    df_JDE_F0116 = JDE_F0116(spark)
    df_JDE_F0006_FILTER = JDE_F0006_FILTER(spark, df_JDE_F0006)
    df_JDE_F0116_FILTER = JDE_F0116_FILTER(spark, df_JDE_F0116)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_PLNT_DEU_DJD_SJD_JEM_JSW_JET_JES_BW2_GMD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_PLNT_DEU_DJD_SJD_JEM_JSW_JET_JES_BW2_GMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
