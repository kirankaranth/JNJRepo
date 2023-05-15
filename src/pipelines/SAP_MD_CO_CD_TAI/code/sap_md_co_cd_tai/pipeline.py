from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_co_cd_tai.config.ConfigStore import *
from sap_md_co_cd_tai.udfs.UDFs import *
from prophecy.utils import *
from sap_md_co_cd_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_TAI_KNB1 = DS_SAP_TAI_KNB1(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_CO_CD_TAI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_CO_CD_TAI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
