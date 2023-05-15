from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_bba_bbn_hcs_mrs_bwi.config.ConfigStore import *
from sap_md_co_cd_bba_bbn_hcs_mrs_bwi.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_bba_bbn_hcs_mrs_bwi.graph import *

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_BBA_BBN_HCS_MRS_BWI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_BBA_BBN_HCS_MRS_BWI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
