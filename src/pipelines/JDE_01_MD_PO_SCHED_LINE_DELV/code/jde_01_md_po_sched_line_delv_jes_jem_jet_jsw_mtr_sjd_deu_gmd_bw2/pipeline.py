from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.config.ConfigStore import *
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_po_sched_line_delv_jes_jem_jet_jsw_mtr_sjd_deu_gmd_bw2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4311 = JDE_F4311(spark)
    df_JDE_F4311 = collectMetrics(
        spark, 
        df_JDE_F4311, 
        "graph", 
        "NQ3EXKL5ryPsVA7Lu3C__$$UgNXQUVBWDdzPFXI_m1ok", 
        "1tyFe1oJvJZHtnOgFsg6n$$Sv5ze5bZ9-9kZNw0y0oE1"
    )
    df_Filter_1 = Filter_1(spark, df_JDE_F4311)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_Filter_1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "XvO7xwnnjgMK3FX-y5qnW$$8UmCaF7agjgm4XSNZZRiN", 
        "IbLewQEzTESMj0SDRJ7RH$$6W-q5M6Yn8z3uWFB5jwZv"
    )
    MD_PO_SCHED_LINE_DELV(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_GET_DUP = GET_DUP(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUP_FILTER = DUP_FILTER(spark, df_GET_DUP)
    df_DUP_FILTER = collectMetrics(
        spark, 
        df_DUP_FILTER, 
        "graph", 
        "W4XbMvTuqnzUFeM1WFpWu$$HbgNZN-0COwbNi7QVwPjz", 
        "qQ-iHk2rvQmVcOiEMjsy5$$oFez5sC5wieVlnEPjICMh"
    )
    df_DUP_FILTER.cache().count()
    df_DUP_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_PO_SCHED_LINE_DELV")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_PO_SCHED_LINE_DELV")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
