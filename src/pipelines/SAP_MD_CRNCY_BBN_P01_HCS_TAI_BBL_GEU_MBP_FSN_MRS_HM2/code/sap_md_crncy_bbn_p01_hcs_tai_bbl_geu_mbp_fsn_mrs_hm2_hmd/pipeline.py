from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.config.ConfigStore import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CRNCY_BBN_P01_HCS_TAI_BBL_GEU_MBP_FSN_MRS_HM2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CRNCY_BBN_P01_HCS_TAI_BBL_GEU_MBP_FSN_MRS_HM2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
