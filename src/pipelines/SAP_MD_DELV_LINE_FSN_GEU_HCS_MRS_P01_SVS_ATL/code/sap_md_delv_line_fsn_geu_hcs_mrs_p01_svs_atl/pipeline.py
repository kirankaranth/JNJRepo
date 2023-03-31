from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.config.ConfigStore import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.graph import *

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_DELV_LINE_FSN_GEU_HCS_MRS_P01_SVS_ATL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_DELV_LINE_FSN_GEU_HCS_MRS_P01_SVS_ATL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
