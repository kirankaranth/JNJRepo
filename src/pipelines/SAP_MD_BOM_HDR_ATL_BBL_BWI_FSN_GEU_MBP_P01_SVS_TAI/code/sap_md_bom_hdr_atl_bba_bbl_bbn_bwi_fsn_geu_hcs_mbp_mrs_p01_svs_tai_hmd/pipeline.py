from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_bom_hdr_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.config.ConfigStore import *
from sap_md_bom_hdr_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_bom_hdr_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_02_STKO = SAP_02_STKO(spark)
    df_SAP_02_STKO = collectMetrics(
        spark, 
        df_SAP_02_STKO, 
        "graph", 
        "Vb0ih8RVkkxtTan7ZwXCC$$gzizT6LWJyPBquXbh9I_t", 
        "03hTSgV4879cHzl8WDWfE$$iuZSAIVU4G-LTj9rv6x05"
    )
    df_STKO_MANDT_FILTER = STKO_MANDT_FILTER(spark, df_SAP_02_STKO)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_STKO_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "VpYiYbRh_mlN2PLVfdoB7$$wwRjWHTsuV_Z6KOKrcq3R", 
        "U1We3gvPbxDdP08PBmCCG$$EnbCkukiNqv3Q63ljBzKC"
    )
    df_PK_COUNT = PK_COUNT(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_PK_GT_1 = PK_GT_1(spark, df_PK_COUNT)
    df_PK_GT_1 = collectMetrics(
        spark, 
        df_PK_GT_1, 
        "graph", 
        "HHobQjn3bgytR4scK33Ts$$hJ9FX8fIc4cD-unM5lbft", 
        "KsFlHyLvW2mW4n-SmdYHY$$w_kLkLG4_zc23Oh7rv7fT"
    )
    df_PK_GT_1.cache().count()
    df_PK_GT_1.unpersist()
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
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_BOM_HDR_ATL_BBL_BWI_FSN_GEU_MBP_P01_SVS_TAI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_BOM_HDR_ATL_BBL_BWI_FSN_GEU_MBP_P01_SVS_TAI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
