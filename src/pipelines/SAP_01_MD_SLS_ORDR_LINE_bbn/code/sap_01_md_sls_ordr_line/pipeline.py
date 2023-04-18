from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_sls_ordr_line.config.ConfigStore import *
from sap_01_md_sls_ordr_line.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_sls_ordr_line.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_TVRO = SAP_TVRO(spark)
    df_MANDT_FILTER_1_1_1_2 = MANDT_FILTER_1_1_1_2(spark, df_SAP_TVRO)
    LU_SAP_TVRO(spark, df_MANDT_FILTER_1_1_1_2)
    df_SAP_TVROT = SAP_TVROT(spark)
    df_MANDT_FILTER_1_1_1_2_1_1_1_1_1_1_1 = MANDT_FILTER_1_1_1_2_1_1_1_1_1_1_1(spark, df_SAP_TVROT)
    LU_SAP_TVROT(spark, df_MANDT_FILTER_1_1_1_2_1_1_1_1_1_1_1)
    df_SAP_TVAGT = SAP_TVAGT(spark)
    df_MANDT_FILTER_1_1_1_1 = MANDT_FILTER_1_1_1_1(spark, df_SAP_TVAGT)
    LU_SAP_TVAGT(spark, df_MANDT_FILTER_1_1_1_1)
    df_SAP_TVM1T = SAP_TVM1T(spark)
    df_MANDT_FILTER_1_1_1_2_1 = MANDT_FILTER_1_1_1_2_1(spark, df_SAP_TVM1T)
    LU_SAP_TVM1T(spark, df_MANDT_FILTER_1_1_1_2_1)
    df_SAP_TVM2T = SAP_TVM2T(spark)
    df_MANDT_FILTER_1_1_1_2_1_1 = MANDT_FILTER_1_1_1_2_1_1(spark, df_SAP_TVM2T)
    LU_SAP_TVM2T(spark, df_MANDT_FILTER_1_1_1_2_1_1)
    df_SAP_TVSTT = SAP_TVSTT(spark)
    df_MANDT_FILTER_1_1_1_2_1_1_1_1_1 = MANDT_FILTER_1_1_1_2_1_1_1_1_1(spark, df_SAP_TVSTT)
    LU_SAP_TVSTT(spark, df_MANDT_FILTER_1_1_1_2_1_1_1_1_1)
    df_SAP_TVAPT = SAP_TVAPT(spark)
    df_MANDT_FILTER_1_1 = MANDT_FILTER_1_1(spark, df_SAP_TVAPT)
    LU_SAP_TVAPT(spark, df_MANDT_FILTER_1_1)
    df_SAP_TVM5T = SAP_TVM5T(spark)
    df_MANDT_FILTER_1_1_1_2_1_1_1_1 = MANDT_FILTER_1_1_1_2_1_1_1_1(spark, df_SAP_TVM5T)
    LU_SAP_TVM5T(spark, df_MANDT_FILTER_1_1_1_2_1_1_1_1)
    df_SAP_VBAP = SAP_VBAP(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VBAP)
    df_SAP_VBAK = SAP_VBAK(spark)
    df_MANDT_FILTER_1 = MANDT_FILTER_1(spark, df_SAP_VBAK)
    df_SAP_VBKD = SAP_VBKD(spark)
    df_MANDT_FILTER_1_1_1 = MANDT_FILTER_1_1_1(spark, df_SAP_VBKD)
    df_SAP_TVST = SAP_TVST(spark)
    df_MANDT_FILTER_1_1_1_2_1_1_1_1_1_1 = MANDT_FILTER_1_1_1_2_1_1_1_1_1_1(spark, df_SAP_TVST)
    df_Join_1 = Join_1(
        spark, 
        df_MANDT_FILTER, 
        df_MANDT_FILTER_1, 
        df_MANDT_FILTER_1_1_1, 
        df_MANDT_FILTER_1_1_1_2_1_1_1_1_1_1
    )
    df_NEW_FIEDS = NEW_FIEDS(spark, df_Join_1)
    MD_SLS_ORDR_LINE(spark, df_NEW_FIEDS)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_NEW_FIEDS)
    df_DUPLICATE_CHECK_1 = DUPLICATE_CHECK_1(spark, df_DUPLICATE_CHECK)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_SLS_ORDR_LINE_bbn")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_SLS_ORDR_LINE_bbn")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()