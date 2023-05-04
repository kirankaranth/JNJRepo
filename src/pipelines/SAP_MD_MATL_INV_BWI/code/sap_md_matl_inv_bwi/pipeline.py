from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_inv_bwi.config.ConfigStore import *
from sap_md_matl_inv_bwi.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_inv_bwi.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_005_MARA = DS_SAP_005_MARA(spark)
    df_DS_SAP_005_MARA = collectMetrics(
        spark, 
        df_DS_SAP_005_MARA, 
        "graph", 
        "zQeFlMRATUHDATlVmKnbF$$ljhpSwJqp-ceA5LHMyKyH", 
        "sYtSVk2q9sfJq89d59kPC$$uwmR95vlmXB4Ekch2-dTy"
    )
    df_MANDT_5 = MANDT_5(spark, df_DS_SAP_005_MARA)
    Lookup_1_MARA(spark, df_MANDT_5)
    df_DS_SAP_001_MARD = DS_SAP_001_MARD(spark)
    df_DS_SAP_001_MARD = collectMetrics(
        spark, 
        df_DS_SAP_001_MARD, 
        "graph", 
        "k5tx1sufcb02v3Btwq2VI$$T2Kdyhfmct-ZP_P5LmKkJ", 
        "3CrnM5rIp04ipW5DJukD9$$MBoUj4bDaXIF3R2Z5W2fM"
    )
    df_MANDT_1 = MANDT_1(spark, df_DS_SAP_001_MARD)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark, df_MANDT_1)
    df_SET_FIELD_ORDER_1 = SET_FIELD_ORDER_1(spark, df_SchemaTransform_1_MARD)
    df_DS_SAP_002_MCHB = DS_SAP_002_MCHB(spark)
    df_DS_SAP_002_MCHB = collectMetrics(
        spark, 
        df_DS_SAP_002_MCHB, 
        "graph", 
        "XuRQrciP3wbKNfte9XJrB$$PZyRMNSnT9trhQjF3hT7Z", 
        "2CE0qLvskoOXN-3tnf2yw$$S3QTH4i3L4b0kt-OIpfg-"
    )
    df_MANDT_2 = MANDT_2(spark, df_DS_SAP_002_MCHB)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark, df_MANDT_2)
    df_SET_FIELD_ORDER_2 = SET_FIELD_ORDER_2(spark, df_SchemaTransform_2_MCHB)
    df_DS_SAP_004_MSSL = DS_SAP_004_MSSL(spark)
    df_DS_SAP_004_MSSL = collectMetrics(
        spark, 
        df_DS_SAP_004_MSSL, 
        "graph", 
        "EP9y27vNQb5DclX_N1se0$$pGWf7GzZYRz8nnYjsyu4W", 
        "DoqLyVIfU0kJCdG1-zyzr$$o4VTVFc--qBf9R7H5zY2D"
    )
    df_MANDT_4 = MANDT_4(spark, df_DS_SAP_004_MSSL)
    df_SchemaTransform_4_MSSL = SchemaTransform_4_MSSL(spark, df_MANDT_4)
    df_SET_FIELD_ORDER_4 = SET_FIELD_ORDER_4(spark, df_SchemaTransform_4_MSSL)
    df_DS_SAP_003_MSLB = DS_SAP_003_MSLB(spark)
    df_DS_SAP_003_MSLB = collectMetrics(
        spark, 
        df_DS_SAP_003_MSLB, 
        "graph", 
        "-xJCyNoZ6_iAkNgrpbK-J$$dsvsPHB8FtyQhNx3WddvP", 
        "Bzh0-f39qLTOGxgov8-eB$$KoTcE351jqpLBK5lwQ3wz"
    )
    df_MANDT_3 = MANDT_3(spark, df_DS_SAP_003_MSLB)
    df_SchemaTransform_3_MSLB = SchemaTransform_3_MSLB(spark, df_MANDT_3)
    df_SET_FIELD_ORDER_3 = SET_FIELD_ORDER_3(spark, df_SchemaTransform_3_MSLB)
    df_SetOperation_1_Union = SetOperation_1_Union(
        spark, 
        df_SET_FIELD_ORDER_1, 
        df_SET_FIELD_ORDER_2, 
        df_SET_FIELD_ORDER_3, 
        df_SET_FIELD_ORDER_4
    )
    df_SetOperation_1_Union = collectMetrics(
        spark, 
        df_SetOperation_1_Union, 
        "graph", 
        "nwL4V5SG2QwdEwD4DFDkd$$-FmVO3Sp2KvLdwJatwxOH", 
        "XtrZ48JMRs66FjVvqqQWU$$nlC25uwzFvtows4ikU79y"
    )
    MD_MATL_INV(spark, df_SetOperation_1_Union)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_INV_BWI")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_INV_BWI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
