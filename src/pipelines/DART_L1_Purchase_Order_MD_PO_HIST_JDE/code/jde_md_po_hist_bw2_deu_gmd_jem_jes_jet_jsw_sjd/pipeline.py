from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_po_hist_bw2_deu_gmd_jem_jes_jet_jsw_sjd.config.ConfigStore import *
from jde_md_po_hist_bw2_deu_gmd_jem_jes_jet_jsw_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_po_hist_bw2_deu_gmd_jem_jes_jet_jsw_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F43199 = JDE_F43199(spark)
    df_JDE_F43199 = collectMetrics(
        spark, 
        df_JDE_F43199, 
        "graph", 
        "Pp393Z8UgE4i8KuWbCSdU$$C0zt4PPh1y3yeORQhWuPh", 
        "18BcPTShZHz6MqK2IKg3B$$NoVpji6eI8uB5QcEZ9F3R"
    )
    df_DELETED_FILTER = DELETED_FILTER(spark, df_JDE_F43199)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_DELETED_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS)
    df_SET_FIELD_ORDER_REFORMAT_1 = SET_FIELD_ORDER_REFORMAT_1(spark, df_DEDUPLICATE)
    df_SET_FIELD_ORDER_REFORMAT_1 = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT_1, 
        "graph", 
        "dhxNcEF121EYVNDbAUf_P$$2Rl6o58OWIxwxo6QMKZPg", 
        "yn4awrewhjRyKN0MZSlYK$$j6xRF1AapDHo9MuYtBLPa"
    )
    df_DUPLICATE = DUPLICATE(spark, df_SET_FIELD_ORDER_REFORMAT_1)
    df_DUP_FILTER = DUP_FILTER(spark, df_DUPLICATE)
    df_DUP_FILTER = collectMetrics(
        spark, 
        df_DUP_FILTER, 
        "graph", 
        "i-TCzO6zWhE-PuQQMNBxg$$gJ-4lMIxzTFtdhb1ini2l", 
        "6F7EO_ax-2235TV1NRUDC$$qUvdnzlsRI6RBcbYImgza"
    )
    df_DUP_FILTER.cache().count()
    df_DUP_FILTER.unpersist()
    MD_PO_HIST(spark, df_SET_FIELD_ORDER_REFORMAT_1)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/DART_L1_Purchase_Order_MD_PO_HIST_JDE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/DART_L1_Purchase_Order_MD_PO_HIST_JDE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
