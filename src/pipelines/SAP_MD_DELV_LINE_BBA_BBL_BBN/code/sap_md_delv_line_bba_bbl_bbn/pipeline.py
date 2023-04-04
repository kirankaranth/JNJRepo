from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_line_bba_bbl_bbn.config.ConfigStore import *
from sap_md_delv_line_bba_bbl_bbn.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_line_bba_bbl_bbn.graph import *

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_DELV_LINE_BBA_BBL_BBN")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_DELV_LINE_BBA_BBL_BBN")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
