from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.config.ConfigStore import *
from sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_MAST = SAP_MAST(spark)
    df_SAP_MAST = collectMetrics(
        spark, 
        df_SAP_MAST, 
        "graph", 
        "bkj91johPWh9Z3QTgO05q$$OAD2fVKXC2pzEuzrt2oMk", 
        "nUX0HtmbgfvkOhe2q8Qga$$XA_KdB88nh7tr8UvA3m4H"
    )
    df_Filter_SAP_01_MAST = Filter_SAP_01_MAST(spark, df_SAP_MAST)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Filter_SAP_01_MAST)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "m1rOU8KVupbUiAZLIlC7o$$jAV9xSse4mX0fxPOkSY2j", 
        "LAHRfyzZWx1Q0S-bBhypc$$Zr36mgDaJPFd4n0aCewcW"
    )
    MD_MATL_BOM(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_BOM")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_BOM")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
