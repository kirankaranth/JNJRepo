from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from jde_md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.graph import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL_INV_BW2_DEU_DJD_GMD_JEM_JES_JET_JSW_MTR_SDJ")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/JDE_MD_MATL_INV_BW2_DEU_DJD_GMD_JEM_JES_JET_JSW_MTR_SDJ"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
