from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_loc.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_MARC = SAP_MARC(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_MARC)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    MD_MATL_LOC(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_MATL_LOC_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_MATL_LOC_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
