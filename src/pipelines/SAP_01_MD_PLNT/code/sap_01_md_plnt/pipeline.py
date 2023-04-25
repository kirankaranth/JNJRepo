from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_plnt.config.ConfigStore import *
from sap_01_md_plnt.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_plnt.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_T005T = SAP_T005T(spark)
    df_SAP_T005T = collectMetrics(
        spark, 
        df_SAP_T005T, 
        "graph", 
        "Io2BnH_yCLHYgk_uMOGbq$$aYTDKU40XlGH4k6c3GQL4", 
        "crYdTEGWB4IfUKd0mrZIy$$c-YM4B1CQj_GTPpqfBB79"
    )
    df_MANDT_FILTER_T005T = MANDT_FILTER_T005T(spark, df_SAP_T005T)
    LU_SAP_T005T(spark, df_MANDT_FILTER_T005T)
    df_SAP_T001K = SAP_T001K(spark)
    df_SAP_T001K = collectMetrics(
        spark, 
        df_SAP_T001K, 
        "graph", 
        "KFL4qV8chcd0_bL50n1W8$$swmBT0sSCrX11zrLcjuns", 
        "lKTMJHsll_4OTkF04smty$$ts_vK1IUxW6D2ezDpYcq5"
    )
    df_SAP_T001W = SAP_T001W(spark)
    df_SAP_T001W = collectMetrics(
        spark, 
        df_SAP_T001W, 
        "graph", 
        "t5FhbUfGhh8JCGf16zK2J$$Cq5BB-Ysw8-zslL2mYPxO", 
        "j1huw-d1AD66QXkMFaJ7p$$WSr7SvGSRbuPD3Ct6N18j"
    )
    df_MANDT_FILTER_T001W = MANDT_FILTER_T001W(spark, df_SAP_T001W)
    df_MANDT_FILTER_T001K = MANDT_FILTER_T001K(spark, df_SAP_T001K)
    df_Join_1 = Join_1(spark, df_MANDT_FILTER_T001W, df_MANDT_FILTER_T001K)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "JunzaeSEOTP42SedOc_xY$$wXUAm2qJF-rfLVdZScLFy", 
        "oFAl0saCzvXJe0rgzYJje$$r1qD6ApQDZy2eqbkXrVci"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_FILTER = DUPLICATE_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_FILTER, 
        "graph", 
        "l5AbQy3wTrDOQsBUkmmY-$$ptKHSYjt6lK5_M4AJcM8m", 
        "VjOiP6WoDVjrKd5r2AFyQ$$aq1KoxD31YmEc7rA6C_Em"
    )
    df_DUPLICATE_FILTER.cache().count()
    df_DUPLICATE_FILTER.unpersist()
    MD_PLNT(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_PLNT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_PLNT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
