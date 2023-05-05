from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.config.ConfigStore import *
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.udfs.UDFs import *
from prophecy.utils import *
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F3002_ADT = JDE_F3002_ADT(spark)
    df_JDE_F3002_ADT = collectMetrics(
        spark, 
        df_JDE_F3002_ADT, 
        "graph", 
        "4W3xXLpb9TKm4ObsEcJf5$$oM4QbH6dWecZS3Zvrvyf5", 
        "CG2Fsi16jtfOHV_Tk__SH$$MuWCA1T_AOOleQ8K94RqH"
    )
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_JDE_F3002_ADT)
    df_Deduplicate = Deduplicate(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_Deduplicate)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "uwWqu0eqg0Nu_fBGLhpZs$$YePWfYvwvLYU0qV8FzWwE", 
        "qWb6r_CwMbKG6zaeG5wp4$$hGz9hID1IuX2BEi8cKyo9"
    )
    df_PK_COUNT = PK_COUNT(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_PK_GT_1 = PK_GT_1(spark, df_PK_COUNT)
    df_PK_GT_1 = collectMetrics(
        spark, 
        df_PK_GT_1, 
        "graph", 
        "k46qxIUE66YLNcAVFsRPi$$vsDdz0Tbj_WxJkmwpoCxw", 
        "yq0fhVp0t2FMIVgtRFAwg$$pMXubjADxdjO0PUerproM"
    )
    df_PK_GT_1.cache().count()
    df_PK_GT_1.unpersist()
    MD_BOM_HDR(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_BOM_HDR_BW2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_BOM_HDR_BW2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
