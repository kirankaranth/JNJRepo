from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.config.ConfigStore import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F43199 = DS_JDE_01_F43199(spark)
    df_DS_JDE_01_F43199 = collectMetrics(
        spark, 
        df_DS_JDE_01_F43199, 
        "graph", 
        "ig7QtwBpA1jc7vdPoaKG1$$skDzavHXwGq82F0N3fcoM", 
        "GUzfR9pWpujSGFtAHtsof$$uDKaf0FlfCX1NY2f5iveh"
    )
    df_FILTER = FILTER(spark, df_DS_JDE_01_F43199)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS)
    df_FIELDS_ORDER_REFORMAT = FIELDS_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_FIELDS_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_FIELDS_ORDER_REFORMAT, 
        "graph", 
        "BJGrM-dbKVctZAVQ6oI-e$$H27Oe0zk8NnNrj7_WmPmF", 
        "1yh91bgJbUhkR_kOBbaOw$$FCd-7izOLJTC0qWRut9u7"
    )
    MD_DOC_ITM_INVC(spark, df_FIELDS_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/jde_md_doc_itm_incm_invc")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/jde_md_doc_itm_incm_invc")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
