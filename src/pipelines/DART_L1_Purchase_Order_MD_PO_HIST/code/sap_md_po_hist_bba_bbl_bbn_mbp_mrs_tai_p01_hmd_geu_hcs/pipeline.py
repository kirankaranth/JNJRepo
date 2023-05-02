from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_po_hist_bba_bbl_bbn_mbp_mrs_tai_p01_hmd_geu_hcs.config.ConfigStore import *
from sap_md_po_hist_bba_bbl_bbn_mbp_mrs_tai_p01_hmd_geu_hcs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_po_hist_bba_bbl_bbn_mbp_mrs_tai_p01_hmd_geu_hcs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_EKBE = SAP_EKBE(spark)
    df_SAP_EKBE = collectMetrics(
        spark, 
        df_SAP_EKBE, 
        "graph", 
        "Pp393Z8UgE4i8KuWbCSdU$$C0zt4PPh1y3yeORQhWuPh", 
        "18BcPTShZHz6MqK2IKg3B$$NoVpji6eI8uB5QcEZ9F3R"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_EKBE)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "R_cdCwo-cwArNbluLZ08V$$qwo3SDeQtzCP0n6TjdBCH", 
        "vpf0VdDt2uar_mMd5rRsA$$moblNbqELx_V6qWJy-SW-"
    )
    df_DUPLICATE = DUPLICATE(spark, df_SET_FIELD_ORDER_REFORMAT)
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
    MD_PO_HIST(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/DART_L1_Purchase_Order_MD_PO_HIST")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/DART_L1_Purchase_Order_MD_PO_HIST")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
