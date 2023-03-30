from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_inv.config.ConfigStore import *
from sap_01_md_matl_inv.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_inv.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_NSDM_V_MARD = DS_SAP_01_NSDM_V_MARD(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_MATL_INV")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_MATL_INV")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
