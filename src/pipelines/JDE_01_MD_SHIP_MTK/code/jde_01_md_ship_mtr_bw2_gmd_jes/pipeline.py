from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_ship_mtr_bw2_gmd_jes.config.ConfigStore import *
from jde_01_md_ship_mtr_bw2_gmd_jes.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_01_md_ship_mtr_bw2_gmd_jes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F43121 = JDE_F43121(spark)
    df_JDE_F43121 = collectMetrics(
        spark, 
        df_JDE_F43121, 
        "graph", 
        "IzhVdU3oa23HO8bEkx-Gz$$gk77k3SIpY7EjYArSrEnR", 
        "3_g5GnOFGth3qW1Nhb-Aj$$lZIEvB6aa7dcRpdZLehVV"
    )
    df_JDE_F43121_FILTER = JDE_F43121_FILTER(spark, df_JDE_F43121)
    df_NEW_FIELDS_RENAME_FORMAT_2 = NEW_FIELDS_RENAME_FORMAT_2(spark, df_JDE_F43121_FILTER)
    df_SET_FIELD_ORDER_FORMAT_1 = SET_FIELD_ORDER_FORMAT_1(spark, df_NEW_FIELDS_RENAME_FORMAT_2)
    df_JDE_F4211 = JDE_F4211(spark)
    df_JDE_F4211 = collectMetrics(
        spark, 
        df_JDE_F4211, 
        "graph", 
        "F2wj1udUgD3DWJNKUCVwG$$6d5wB83Cq9LGpHIyH-Qde", 
        "_LrGjReAfhmr96MB4xghp$$kSB3M9-emCHJDTrmfcq_K"
    )
    df_JDE_F4211_FILTER = JDE_F4211_FILTER(spark, df_JDE_F4211)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_JDE_F4211_FILTER)
    df_SET_FIELD_ORDER_FORMAT = SET_FIELD_ORDER_FORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_Deduplicate_1 = Deduplicate_1(spark, df_SET_FIELD_ORDER_FORMAT_1)
    df_UNION = UNION(spark, df_SET_FIELD_ORDER_FORMAT, df_Deduplicate_1)
    df_UNION = collectMetrics(
        spark, 
        df_UNION, 
        "graph", 
        "eTghCKz-e4caJMnSVz95v$$K4sUmYOe1bmNMwE5pbPUM", 
        "Ty1lOjVSOq88vvmZCxs4L$$W-pQ8-skLz1WzQcWO0wGS"
    )
    MD_SHIP(spark, df_UNION)
    df_DUPLICATE = DUPLICATE(spark, df_UNION)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "wQL-DlR_cGUxQ__6LsV3-$$yBZiNK53ia7QNpqczRL5j", 
        "40XvrkgdISQUjZAmOePX9$$kiIR0zEFpzY22XbS9RwZK"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_SHIP_MTK")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_SHIP_MTK")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
