from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd.config.ConfigStore import *
from jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_f3002_f3002_adt = JDE_f3002_f3002_adt(spark)
    df_JDE_f3002_f3002_adt = collectMetrics(
        spark, 
        df_JDE_f3002_f3002_adt, 
        "graph", 
        "tds-xWzfVXzjUK-9TPNj7$$pzCEwZ9p2chKzQO-gnUtg", 
        "-I1oCENYg5OEMsjQcEiq6$$TlV-gB4016JY4w49MuXBc"
    )
    df_Filter_SAP_01_MAST = Filter_SAP_01_MAST(spark, df_JDE_f3002_f3002_adt)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Filter_SAP_01_MAST)
    df_Deduplicate_1 = Deduplicate_1(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_Deduplicate_1)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "HVC-DUd82Ub7MIxvEnmFD$$iWUftOnvHYzX31OE0tYra", 
        "-waJvJHFfCHpYidfquBF7$$OeLDUB2vkuspvOA1_0PIR"
    )
    df_PK_COUNT = PK_COUNT(spark, df_SET_FIELD_ORDER_REFORMAT)
    MD_MATL_BOM(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_PK_COUNT_GT_1 = PK_COUNT_GT_1(spark, df_PK_COUNT)
    df_PK_COUNT_GT_1 = collectMetrics(
        spark, 
        df_PK_COUNT_GT_1, 
        "graph", 
        "gS1_nfVFIOWtAnyZzUU2l$$s5sFoqWYLUKfnSFhOrrK3", 
        "yWtjXmwHOFXlZwX7Yxnbh$$R2i1glxo-p7yPKtNaky25"
    )
    df_PK_COUNT_GT_1.cache().count()
    df_PK_COUNT_GT_1.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_MATL_BOM")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_MATL_BOM")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
