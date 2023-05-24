from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_bw2_md_matl.config.ConfigStore import *
from jde_bw2_md_matl.udfs.UDFs import *
from prophecy.utils import *
from jde_bw2_md_matl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_F0005_41 = F0005_41(spark)
    df_F0005_41 = collectMetrics(
        spark, 
        df_F0005_41, 
        "graph", 
        "IwqM6en2Wen7_rnocVOGY$$BuM9Pan3pGXCjmGdZnVCw", 
        "pFs-s97IlaM4wdxpnKq5V$$UxCZRkRlHQ2i58EU3pJ_p"
    )
    df_TRIM = TRIM(spark, df_F0005_41)
    df_FRAN_CD = FRAN_CD(spark, df_TRIM)
    FRAN_LU(spark, df_FRAN_CD)
    df_F4104 = F4104(spark)
    df_F4104 = collectMetrics(
        spark, 
        df_F4104, 
        "graph", 
        "AhOldWo28OZaMGRM3kMAF$$YO5cwlI0MI64GZf-pNDGN", 
        "51OcUGRHTy_oQZyUqVD9A$$SCsBUvEj_Lnm-lR5xZH6u"
    )
    df_F6060002 = F6060002(spark)
    df_F6060002 = collectMetrics(
        spark, 
        df_F6060002, 
        "graph", 
        "2D2jL0xHw_StxYwSULvFr$$3y-rUjgKLPWgOtxFyG9c0", 
        "ME-P7RBtsn0Zy_Ue5ZUwr$$iNQ3HEaSwdL-CZlCP2So3"
    )
    df_SHELF_LIFE = SHELF_LIFE(spark, df_F4104, df_F6060002)
    SLD_LU(spark, df_SHELF_LIFE)
    df_BRAVO_MINOR_DESC = BRAVO_MINOR_DESC(spark, df_TRIM)
    BRAVO_D_LU(spark, df_BRAVO_MINOR_DESC)
    df_MATL_TYPE_DESC = MATL_TYPE_DESC(spark, df_TRIM)
    MATL_T_LU(spark, df_MATL_TYPE_DESC)
    df_MATL_GRP = MATL_GRP(spark, df_TRIM)
    MATL_GR_LU(spark, df_MATL_GRP)
    df_DS_JDE_01_F4101 = DS_JDE_01_F4101(spark)
    df_DS_JDE_01_F4101 = collectMetrics(
        spark, 
        df_DS_JDE_01_F4101, 
        "graph", 
        "KmcspySnyqS3B1jCwN5Wl$$fxxc3NjEQ-IkKVuSdMNK4", 
        "34drVHGEIPl8judmVhk_D$$5JpFkTtTM1UlfzRafC2Uj"
    )
    df_F4101_SELECTION = F4101_SELECTION(spark, df_DS_JDE_01_F4101)
    df_DEL = DEL(spark, df_F4101_SELECTION)
    df_XFORM = XFORM(spark, df_DEL)
    df_FIELD_ORDER = FIELD_ORDER(spark, df_XFORM)
    df_FIELD_ORDER = collectMetrics(
        spark, 
        df_FIELD_ORDER, 
        "graph", 
        "tEaJK5khO4iuuMg5fTf5n$$t5sw1Fvn9AEcqoSGAOuIE", 
        "X5_yIddhVngEUR5nfyn0E$$vfGy99rUws5XAaiMCPZQ0"
    )
    TARGET(spark, df_FIELD_ORDER)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_BW2_MD_MATL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_BW2_MD_MATL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
