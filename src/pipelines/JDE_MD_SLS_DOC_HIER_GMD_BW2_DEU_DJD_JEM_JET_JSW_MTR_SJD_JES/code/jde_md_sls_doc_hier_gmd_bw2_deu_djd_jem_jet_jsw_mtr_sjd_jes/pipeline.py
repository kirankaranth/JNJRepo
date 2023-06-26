from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.config.ConfigStore import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F42119 = JDE_F42119(spark)
    df_JDE_F42119 = collectMetrics(
        spark, 
        df_JDE_F42119, 
        "graph", 
        "vdWU5dCILVvTa5PTigIEe$$kPwyzvawBncWhd-xzFHU3", 
        "5L8fhkxBOulhL7jA_xC73$$JjFKWJUgAsjelQs_8tBRM"
    )
    df_FILTER_F42119 = FILTER_F42119(spark, df_JDE_F42119)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_FILTER_F42119)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_NEW_FIELDS)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS)
    df_FIELD_ORDER = FIELD_ORDER(spark, df_DEDUPLICATE)
    df_FIELD_ORDER = collectMetrics(
        spark, 
        df_FIELD_ORDER, 
        "graph", 
        "mbo372BCjrH4ktmOu0GvL$$31O7kjFk910TEdbPM9BhX", 
        "yBZ3ibMw1wa8b6lrUdCkd$$THofUnokYFVqSDSsHMJ5g"
    )
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "bHNM7e7-ELU5IRWpb7Ffb$$GrDEszsE4g3LucMlShejz", 
        "Tm0xhfyJtBYT8lDV-FJ6J$$OVdcXmswZ3-KSEYAd2kTA"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
    MD_SLS_DOC_HIER(spark, df_FIELD_ORDER)

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
    spark.conf.set(
        "prophecy.metadata.pipeline.uri",
        "pipelines/JDE_MD_SLS_DOC_HIER_GMD_BW2_DEU_DJD_JEM_JET_JSW_MTR_SJD_JES"
    )
    registerUDFs(spark)
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/JDE_MD_SLS_DOC_HIER_GMD_BW2_DEU_DJD_JEM_JET_JSW_MTR_SJD_JES"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
