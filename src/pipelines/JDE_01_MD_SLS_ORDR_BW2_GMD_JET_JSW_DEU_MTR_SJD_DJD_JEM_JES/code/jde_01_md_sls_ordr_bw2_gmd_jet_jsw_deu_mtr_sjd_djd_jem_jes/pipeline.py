from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4201 = JDE_F4201(spark)
    df_JDE_F4201_FILTER = JDE_F4201_FILTER(spark, df_JDE_F4201)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_JDE_F4201_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
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
