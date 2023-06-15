from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw_jem_mtr.config.ConfigStore import *
from jde_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw_jem_mtr.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw_jem_mtr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_f4101 = DS_JDE_01_f4101(spark)
    df_DS_JDE_01_f4101 = collectMetrics(
        spark, 
        df_DS_JDE_01_f4101, 
        "graph", 
        "LHexxiar3F_q6eWtpg2rT$$busrFhnHk9vX13VXRDZJI", 
        "LgM5PnRq6QPFL9_0CmFQU$$UKIhXXqe_2uRg1T7MkX4M"
    )
    df_FILTER = FILTER(spark, df_DS_JDE_01_f4101)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS)
    df_ORDER = ORDER(spark, df_DEDUPLICATE)
    df_ORDER = collectMetrics(
        spark, 
        df_ORDER, 
        "graph", 
        "PvGwDDwRJH0ORNLr0RmaI$$ghsrRUNnEMj3Dt_uRmkJq", 
        "oGbkzxtmzhGqiTKuJZhqu$$r_TP0K639NbXnfWm4lhOX"
    )
    MD_MATL_DSTN_CHN(spark, df_ORDER)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_md_matl_dstn_chn")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_md_matl_dstn_chn")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
