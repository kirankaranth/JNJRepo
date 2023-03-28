from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_bom_hdr_bw2.config.ConfigStore import *
from jde_md_bom_hdr_bw2.udfs.UDFs import *
from prophecy.utils import *
from jde_md_bom_hdr_bw2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F3002_ADT = JDE_F3002_ADT(spark)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_JDE_F3002_ADT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    MD_BOM_HDR(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_BOM_HDR_BW2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_BOM_HDR_BW2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
