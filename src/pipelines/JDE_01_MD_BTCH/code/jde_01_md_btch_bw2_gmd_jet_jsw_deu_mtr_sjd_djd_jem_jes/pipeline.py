from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4108 = JDE_F4108(spark)
    df_JDE_F4108 = collectMetrics(
        spark, 
        df_JDE_F4108, 
        "graph", 
        "8fMrcCUOLSI_2oqqv4c51$$WrIKhr57mhf0ta4mVv4Wn", 
        "P-RtSVV4grUBi1snAleZi$$pIT6KHsudM7q1u2gBoVgU"
    )
    df_F4108_FILTER = F4108_FILTER(spark, df_JDE_F4108)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_F4108_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS)
    df_SET_FIELDS_ORDER = SET_FIELDS_ORDER(spark, df_DEDUPLICATE)
    df_SET_FIELDS_ORDER = collectMetrics(
        spark, 
        df_SET_FIELDS_ORDER, 
        "graph", 
        "rO4zQy9Tr-kfBefGKlxGh$$BEqz7GpL9S0J3yRR41NLJ", 
        "Bdcz2WAZGigLd0elgXH9D$$c_p4Byw9EMhXHE57SSbhH"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELDS_ORDER)
    MD_BTCH(spark, df_SET_FIELDS_ORDER)
    df_DUPLICATE_FILTER = DUPLICATE_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_FILTER, 
        "graph", 
        "6evo4pBimFnen-ELJbkkp$$Oo6ifTQrn7Ty_wEJ1r9dT", 
        "VQkBWC2SM9HBaCLC6JvIe$$Uj9yh9Wr2_mrAD1z5ZZil"
    )
    df_DUPLICATE_FILTER.cache().count()
    df_DUPLICATE_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_BTCH")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_BTCH")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
