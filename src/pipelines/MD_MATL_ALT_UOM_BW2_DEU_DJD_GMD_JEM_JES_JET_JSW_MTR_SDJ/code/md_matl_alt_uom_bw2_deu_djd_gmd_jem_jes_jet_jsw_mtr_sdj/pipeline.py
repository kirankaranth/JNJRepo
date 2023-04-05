from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *
from prophecy.utils import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F4101 = DS_JDE_01_F4101(spark)
    df_DS_JDE_01_F41002 = DS_JDE_01_F41002(spark)
    df_SELECT_FIELDS_1 = SELECT_FIELDS_1(spark, df_DS_JDE_01_F4101)
    df_Join_1 = Join_1(spark)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_DS_JDE_01_F41002)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_MATL_ALT_UOM_BW2_DEU_DJD_GMD_JEM_JES_JET_JSW_MTR_SDJ")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/MD_MATL_ALT_UOM_BW2_DEU_DJD_GMD_JEM_JES_JET_JSW_MTR_SDJ"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
