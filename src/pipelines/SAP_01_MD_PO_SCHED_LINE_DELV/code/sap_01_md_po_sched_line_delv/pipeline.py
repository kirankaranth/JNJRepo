from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_po_sched_line_delv.config.ConfigStore import *
from sap_01_md_po_sched_line_delv.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_po_sched_line_delv.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_EKET = SAP_EKET(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_PO_SCHED_LINE_DELV")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_PO_SCHED_LINE_DELV")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
