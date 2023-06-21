from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_mvmt_hdr.config.ConfigStore import *
from sap_md_matl_mvmt_hdr.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_mvmt_hdr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_MKPF = DS_SAP_MKPF(spark)
    df_DS_SAP_MKPF = collectMetrics(
        spark, 
        df_DS_SAP_MKPF, 
        "graph", 
        "fsXFuyUyQDKtnlr2PiyIW$$yokVuSqMiVmiG-Idh2aAc", 
        "1yguecDqsPC5B8_tMLq83$$hRmdn2ncsrwxv0tYK4OAq"
    )
    df_FILTER_MANDT = FILTER_MANDT(spark, df_DS_SAP_MKPF)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_FILTER_MANDT)
    df_SET_FIELDS_OUTPUT = SET_FIELDS_OUTPUT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELDS_OUTPUT = collectMetrics(
        spark, 
        df_SET_FIELDS_OUTPUT, 
        "graph", 
        "GK8pcmJ3DuZZmVvTL7l0q$$_bxzNN0oVOq3VNlKqPCeO", 
        "DdpgcZLnbJXR3yX0cJVKs$$w_enyWj-34gdoP4C-J8_U"
    )
    MD_MATL_MVMT_HDR(spark, df_SET_FIELDS_OUTPUT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_MVMT_HDR")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_MVMT_HDR")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
