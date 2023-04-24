from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dart_l1_purchase_order_md_po_hist.config.ConfigStore import *
from dart_l1_purchase_order_md_po_hist.udfs.UDFs import *
from prophecy.utils import *
from dart_l1_purchase_order_md_po_hist.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_EKBE = SAP_EKBE(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_EKBE)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_DUPLICATE = DUPLICATE(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUP_FILTER = DUP_FILTER(spark, df_DUPLICATE)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/DART_L1_Purchase_Order_MD_PO_HIST")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/DART_L1_Purchase_Order_MD_PO_HIST")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
