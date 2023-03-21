from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_po_sched_line_delv.config.ConfigStore import *
from jde_01_md_po_sched_line_delv.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_po_sched_line_delv.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4311 = JDE_F4311(spark)
    df_JDE_F4311 = df_JDE_F4311.cache()
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_JDE_F4311)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_PO_SCHED_LINE_DELV")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_PO_SCHED_LINE_DELV")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
