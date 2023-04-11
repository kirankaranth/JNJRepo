from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *
from prophecy.utils import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F43121 = DS_JDE_01_F43121(spark)
    df_DS_JDE_01_F4211 = DS_JDE_01_F4211(spark)
    df_Filter_1 = Filter_1(spark, df_DS_JDE_01_F43121)
    df_Filter_2 = Filter_2(spark, df_DS_JDE_01_F4211)

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
