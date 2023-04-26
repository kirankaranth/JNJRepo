from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *
from prophecy.utils import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F4101 = DS_JDE_01_F4101(spark)
    df_DS_JDE_01_F4101 = collectMetrics(
        spark, 
        df_DS_JDE_01_F4101, 
        "graph", 
        "jhWAIQ2lr-N0XBZ7aNiZS$$Hx-x4t92qRfqZz7OUiYk6", 
        "USflqvPYmtWT40hK7509j$$GuyLH_elitEHPgEC3JtAY"
    )
    df_DEL_FILTER1 = DEL_FILTER1(spark, df_DS_JDE_01_F4101)
    IMLITM_LU(spark, df_DEL_FILTER1)
    IMUOM1_LU(spark, df_DEL_FILTER1)
    df_DS_JDE_01_F41002 = DS_JDE_01_F41002(spark)
    df_DS_JDE_01_F41002 = collectMetrics(
        spark, 
        df_DS_JDE_01_F41002, 
        "graph", 
        "zAoLFoDw4_Lk76YaX4Ak1$$GtYZtltS0fGFQUC5enVQJ", 
        "yW2d4sx3OjamuvhxvdVpB$$w0MIG1cophxzxehXMFejO"
    )
    df_DEL_FILTER = DEL_FILTER(spark, df_DS_JDE_01_F41002)
    df_DISTINCT_ITEM = DISTINCT_ITEM(spark, df_DEL_FILTER)
    df_ITEM_BASE_UOM = ITEM_BASE_UOM(spark, df_DISTINCT_ITEM)
    df_UMRUM_JOIN = UMRUM_JOIN(spark, df_ITEM_BASE_UOM, df_DEL_FILTER)
    df_UMUM_JOIN = UMUM_JOIN(spark, df_ITEM_BASE_UOM, df_DEL_FILTER)
    df_UNION = UNION(spark, df_UMRUM_JOIN, df_UMUM_JOIN)
    df_DE_DUP = DE_DUP(spark, df_UNION)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_DE_DUP)
    df_FINAL_SELECT = FINAL_SELECT(spark, df_SchemaTransform_1)
    df_FINAL_SELECT = collectMetrics(
        spark, 
        df_FINAL_SELECT, 
        "graph", 
        "GPpL70vOG56FhbvEpiDHp$$y36puJMxnHpchxalxgMVK", 
        "r6bWqEeFh0dOkehW2o47n$$gDnxf7PK5bv_kAMX8eS1l"
    )
    df_DUP_CHECK = DUP_CHECK(spark, df_FINAL_SELECT)
    df_DUPS = DUPS(spark, df_DUP_CHECK)
    df_DUPS = collectMetrics(
        spark, 
        df_DUPS, 
        "graph", 
        "C7epOB5wQwsKcInySj7Nj$$z71kHNvjFAjxYIoihd8Wi", 
        "Q4BrIHubTsjjBdBtn0mxP$$UcN54tQybKK4OLer4eLxf"
    )
    df_DUPS.cache().count()
    df_DUPS.unpersist()
    MD_MATL_ALT_UOM(spark, df_FINAL_SELECT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_MATL_ALT_UOM_BW2_DEU_DJD_GMD_JEM_JES_JET_JSW_MTR_SDJ")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/MD_MATL_ALT_UOM_BW2_DEU_DJD_GMD_JEM_JES_JET_JSW_MTR_SDJ"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
