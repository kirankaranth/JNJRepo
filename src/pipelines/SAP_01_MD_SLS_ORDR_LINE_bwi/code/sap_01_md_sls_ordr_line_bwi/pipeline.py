from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_sls_ordr_line_bwi.config.ConfigStore import *
from sap_01_md_sls_ordr_line_bwi.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_sls_ordr_line_bwi.graph import *

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
    df_SAP_TVSTT = SAP_TVSTT(spark)
    df_MANDT_FILTER_1_1_1_2_1_1_1_1_1 = MANDT_FILTER_1_1_1_2_1_1_1_1_1(spark, df_SAP_TVSTT)
    LU_SAP_TVSTT(spark, df_MANDT_FILTER_1_1_1_2_1_1_1_1_1)
    df_SAP_TVAPT = SAP_TVAPT(spark)
    df_MANDT_FILTER_1_1 = MANDT_FILTER_1_1(spark, df_SAP_TVAPT)
    LU_SAP_TVAPT(spark, df_MANDT_FILTER_1_1)
    df_SAP_VBAP = SAP_VBAP(spark)
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VBAP)
    df_nonprodFilter_ERDAT = nonprodFilter_ERDAT(spark, df_MANDT_FILTER)
    df_SAP_VBAK = SAP_VBAK(spark)
    df_MANDT_FILTER_1 = MANDT_FILTER_1(spark, df_SAP_VBAK)
    df_nonprodFilter_ERDAT_1 = nonprodFilter_ERDAT_1(spark, df_MANDT_FILTER_1)
    df_SELECT_VBAK = SELECT_VBAK(spark, df_nonprodFilter_ERDAT_1)
    df_SAP_VBKD = SAP_VBKD(spark)
    df_MANDT_FILTER_1_1_1 = MANDT_FILTER_1_1_1(spark, df_SAP_VBKD)
    df_nonprodFilter_vbkd = nonprodFilter_vbkd(spark, df_MANDT_FILTER_1_1_1)
    df_SELECT_VBKD = SELECT_VBKD(spark, df_nonprodFilter_vbkd)
    df_Join_1 = Join_1(spark, df_nonprodFilter_ERDAT, df_SELECT_VBAK, df_SELECT_VBKD)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_SLS_ORDR_LINE_bwi")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_SLS_ORDR_LINE_bwi")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
