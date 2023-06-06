from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_own_expln_for_term_of_pmt.config.ConfigStore import *
from sap_md_own_expln_for_term_of_pmt.udfs.UDFs import *
from prophecy.utils import *
from sap_md_own_expln_for_term_of_pmt.graph import *

def pipeline(spark: SparkSession) -> None:
    df_T052U = T052U(spark)
    df_T052U = collectMetrics(
        spark, 
        df_T052U, 
        "graph", 
        "eRESEU8h6eiMiDDtTDBsr$$gE-5aF97RYx9My5uSpbCM", 
        "W763JXMJCUaWtZwUQTxXb$$7LaSLW2DDd6nUR7g_rZPm"
    )
    df_DEL = DEL(spark, df_T052U)
    df_XFORM = XFORM(spark, df_DEL)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    df_SELECT_FIELDS = collectMetrics(
        spark, 
        df_SELECT_FIELDS, 
        "graph", 
        "p2w2z52hu1MZZtL86oV1P$$Lb367ghhFJUWBTPm_8RWq", 
        "x6jsk2cAm5wYrlyAGh6sR$$rRvvBvU95yO3hlIxj4yqn"
    )
    PMENT_TS(spark, df_SELECT_FIELDS)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_OWN_EXPLN_FOR_TERM_OF_PMT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_OWN_EXPLN_FOR_TERM_OF_PMT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
