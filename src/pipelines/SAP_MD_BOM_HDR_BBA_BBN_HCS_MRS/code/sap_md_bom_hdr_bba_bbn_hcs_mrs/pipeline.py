from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_bom_hdr_bba_bbn_hcs_mrs.config.ConfigStore import *
from sap_md_bom_hdr_bba_bbn_hcs_mrs.udfs.UDFs import *
from prophecy.utils import *
from sap_md_bom_hdr_bba_bbn_hcs_mrs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_01_STKO = SAP_01_STKO(spark)
    df_SAP_01_STKO = collectMetrics(
        spark, 
        df_SAP_01_STKO, 
        "graph", 
        "sLG6soW8b_shtvgFHOB9_$$WAh-tmzM3LkrmSCqD5o9E", 
        "lkL_EbrDasB97s8tM5sgE$$c1K7_n2t1LsM3hkvrRImA"
    )
    df_STKO_MANDT_FILTER = STKO_MANDT_FILTER(spark, df_SAP_01_STKO)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_STKO_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "igg2iqJQmNsIhc2mofLIj$$fW7b_2eAu9vpSPl1ei21a", 
        "_QGEtwefJ0acNf5J166xx$$aTygr9PvgsQcb5W_KDCOQ"
    )
    MD_BOM_HDR(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_Aggregate_1 = Aggregate_1(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_Filter_1 = Filter_1(spark, df_Aggregate_1)
    df_Filter_1 = collectMetrics(
        spark, 
        df_Filter_1, 
        "graph", 
        "bJXytzaYwyD4zvoSWCpNN$$2n_Q0FcNA8i0zg9R9XOQt", 
        "GWMA4eYDRmisCfi-t1mRy$$wU-Fi9iofoCjFp_frGuTy"
    )
    df_Filter_1.cache().count()
    df_Filter_1.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_BOM_HDR_BBA_BBN_HCS_MRS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_BOM_HDR_BBA_BBN_HCS_MRS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
