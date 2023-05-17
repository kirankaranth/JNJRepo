from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_sls_ordr_hist_jde_deu_djd_jem_jet_sjd.config.ConfigStore import *
from jde_md_sls_ordr_hist_jde_deu_djd_jem_jet_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_sls_ordr_hist_jde_deu_djd_jem_jet_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_F42199 = F42199(spark)
    df_DELETED = DELETED(spark, df_F42199)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_DELETED)
    df_SET_FIELDS_ORDER = SET_FIELDS_ORDER(spark, df_NEW_FIELDS)
    MD_SLS_ORDR_HIST_JDE(spark, df_SET_FIELDS_ORDER)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_SLS_ORDR_HIST_JDE_DEU_DJD_JEM_JET_SJD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_SLS_ORDR_HIST_JDE_DEU_DJD_JEM_JET_SJD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
