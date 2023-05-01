from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.config.ConfigStore import *
from sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.udfs.UDFs import *
from prophecy.utils import *
from sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_RSEG = DS_SAP_RSEG(spark)
    df_DS_SAP_RSEG = collectMetrics(
        spark, 
        df_DS_SAP_RSEG, 
        "graph", 
        "BrkwQbdVP3hITOSUGxj6E$$Le3mC6S9Fy3sOnZFl7y-m", 
        "QYiFm0kQBqhkRc2pScpMC$$lUISyGTT10qzfcQeQGVe8"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_RSEG)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_RSEG")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_RSEG")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
