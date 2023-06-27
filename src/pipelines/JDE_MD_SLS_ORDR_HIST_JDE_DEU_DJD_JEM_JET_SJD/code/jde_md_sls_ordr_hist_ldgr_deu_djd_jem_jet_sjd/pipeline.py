from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_sls_ordr_hist_ldgr_deu_djd_jem_jet_sjd.config.ConfigStore import *
from jde_md_sls_ordr_hist_ldgr_deu_djd_jem_jet_sjd.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_sls_ordr_hist_ldgr_deu_djd_jem_jet_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_F42199 = F42199(spark)
    df_F42199 = collectMetrics(
        spark, 
        df_F42199, 
        "graph", 
        "UsCwLY81iPlWZUzQl0FcV$$WQg7bc8khIWFPJXrcSvGH", 
        "lTthv8hPqbrp7LfrILHcq$$cjdBo3xXOMPZIwLgYcV32"
    )
    df_DELETED = DELETED(spark, df_F42199)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_DELETED)
    df_SET_FIELDS_ORDER = SET_FIELDS_ORDER(spark, df_NEW_FIELDS)
    df_REMOVE_DUPLICATES = REMOVE_DUPLICATES(spark, df_SET_FIELDS_ORDER)
    df_REMOVE_DUPLICATES = collectMetrics(
        spark, 
        df_REMOVE_DUPLICATES, 
        "graph", 
        "LIJC-x2wg5SMtSHJE1CnZ$$wRi-ClAr7q3OK9DBATXUV", 
        "hvinVBnQW7657n4qd6mwI$$xSYdX93OiGPfH2cONKmx-"
    )
    MD_SLS_ORDR_HIST_LDGR(spark, df_REMOVE_DUPLICATES)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_SLS_ORDR_HIST_JDE_DEU_DJD_JEM_JET_SJD")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_SLS_ORDR_HIST_JDE_DEU_DJD_JEM_JET_SJD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
