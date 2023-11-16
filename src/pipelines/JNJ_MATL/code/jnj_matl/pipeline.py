from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jnj_matl.config.ConfigStore import *
from jnj_matl.udfs.UDFs import *
from prophecy.utils import *
from jnj_matl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_T134T_JNJ = DS_SAP_01_T134T_JNJ(spark)
    df_Filter_t134t = Filter_t134t(spark, df_DS_SAP_01_T134T_JNJ)
    SAP_T134T_LU_JNJ(spark, df_Filter_t134t)
    df_DS_SAP_01_CABN_JNJ = DS_SAP_01_CABN_JNJ(spark)
    df_DS_SAP_01_MAKT_JNJ = DS_SAP_01_MAKT_JNJ(spark)
    df_Filter_makt = Filter_makt(spark, df_DS_SAP_01_MAKT_JNJ)
    df_DS_SAP_01_AUSP_JNJ = DS_SAP_01_AUSP_JNJ(spark)
    df_DS_SAP_01_MARA_JNJ = DS_SAP_01_MARA_JNJ(spark)
    df_Filter_mara = Filter_mara(spark, df_DS_SAP_01_MARA_JNJ)
    df_Filter_ausp = Filter_ausp(spark, df_DS_SAP_01_AUSP_JNJ)
    df_DS_SAP_01_INOB_JNJ = DS_SAP_01_INOB_JNJ(spark)
    df_Filter_inob = Filter_inob(spark, df_DS_SAP_01_INOB_JNJ)
    df_Filter_cabn = Filter_cabn(spark, df_DS_SAP_01_CABN_JNJ)
    df_Join_2 = Join_2(spark, df_Filter_ausp, df_Filter_inob, df_Filter_cabn)
    df_Join_1 = Join_1(spark, df_Filter_mara, df_Filter_makt, df_Join_2)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Join_1)
    df_Reformat_1 = Reformat_1(spark, df_SchemaTransform_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JNJ_MATL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JNJ_MATL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
