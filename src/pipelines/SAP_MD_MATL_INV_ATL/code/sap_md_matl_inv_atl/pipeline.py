from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_inv_atl.config.ConfigStore import *
from sap_md_matl_inv_atl.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_inv_atl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_0000006_MARA = DS_SAP_0000006_MARA(spark)
    df_DS_SAP_0000006_MARA = collectMetrics(
        spark, 
        df_DS_SAP_0000006_MARA, 
        "graph", 
        "IJ5NuzifNlRHw1_S3acvn$$XZiimepA2pHCz3CG1qyUi", 
        "MybdmUVzs15xSdR_1-D_J$$odQZABeQQyezVnDG0iPmv"
    )
    df_MANDT_6 = MANDT_6(spark, df_DS_SAP_0000006_MARA)
    Lookup_1_MARA(spark, df_MANDT_6)
    df_DS_SAP_0000002_MCHB = DS_SAP_0000002_MCHB(spark)
    df_DS_SAP_0000002_MCHB = collectMetrics(
        spark, 
        df_DS_SAP_0000002_MCHB, 
        "graph", 
        "YaM1fn39DJoHvnTgoW8Ds$$BNOGRfKYh-LVTCSxrMUPw", 
        "XZsf4J9LDmNZ3SkZGr9ht$$ZGTsBoKFUyxjPf9fIooxC"
    )
    df_DS_SAP_0000004_MSLB = DS_SAP_0000004_MSLB(spark)
    df_DS_SAP_0000004_MSLB = collectMetrics(
        spark, 
        df_DS_SAP_0000004_MSLB, 
        "graph", 
        "oobBQwuiBj7ore1GoFcDm$$2f4Bbd1WMA1CZ9rIMLElD", 
        "8nL-3lUELkgn4haw_OD2h$$iFm3V3TPeEyeJWmb7fan4"
    )
    df_MANDT_4 = MANDT_4(spark, df_DS_SAP_0000004_MSLB)
    df_MANDT_2 = MANDT_2(spark, df_DS_SAP_0000002_MCHB)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark, df_MANDT_2)
    df_SET_FIELD_ORDER_2 = SET_FIELD_ORDER_2(spark, df_SchemaTransform_2_MCHB)
    df_DS_SAP_0000003_MKOL = DS_SAP_0000003_MKOL(spark)
    df_DS_SAP_0000003_MKOL = collectMetrics(
        spark, 
        df_DS_SAP_0000003_MKOL, 
        "graph", 
        "QtliVpMYiwU9jss57BUIB$$0pqUECXCWnZO69-LTkizA", 
        "b_x4_YkQB3KV-r_kyQWZG$$dsxZOaYupJMRTut5U0pae"
    )
    df_MANDT_3 = MANDT_3(spark, df_DS_SAP_0000003_MKOL)
    df_SchemaTransform_3_MKOL = SchemaTransform_3_MKOL(spark, df_MANDT_3)
    df_SET_FIELD_ORDER_3 = SET_FIELD_ORDER_3(spark, df_SchemaTransform_3_MKOL)
    df_DS_SAP_0000005_MSSL = DS_SAP_0000005_MSSL(spark)
    df_DS_SAP_0000005_MSSL = collectMetrics(
        spark, 
        df_DS_SAP_0000005_MSSL, 
        "graph", 
        "b1hWFGoZwCugRsi9bNbo2$$WQz4bUADeUOBCsQSVDWLA", 
        "ACLiT7HycPUHxgS9h73z5$$4gPZAc7x7o3JQI1YnXfpa"
    )
    df_MANDT_5 = MANDT_5(spark, df_DS_SAP_0000005_MSSL)
    df_SchemaTransform_5_MSSL = SchemaTransform_5_MSSL(spark, df_MANDT_5)
    df_SET_FIELD_ORDER_5 = SET_FIELD_ORDER_5(spark, df_SchemaTransform_5_MSSL)
    df_SchemaTransform_4_MSLB = SchemaTransform_4_MSLB(spark, df_MANDT_4)
    df_DS_SAP_0000001_MARD = DS_SAP_0000001_MARD(spark)
    df_DS_SAP_0000001_MARD = collectMetrics(
        spark, 
        df_DS_SAP_0000001_MARD, 
        "graph", 
        "N3kaCSsahMsUWknQ9M67x$$i8bk6KYGJ_gbIcvNKTL3w", 
        "zbp2a-qKqL5YcabsPqTSS$$-_mucTqkssRXA3cW606RA"
    )
    df_MANDT_1 = MANDT_1(spark, df_DS_SAP_0000001_MARD)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark, df_MANDT_1)
    df_SET_FIELD_ORDER_1 = SET_FIELD_ORDER_1(spark, df_SchemaTransform_1_MARD)
    df_SET_FIELD_ORDER_4 = SET_FIELD_ORDER_4(spark, df_SchemaTransform_4_MSLB)
    df_SetOperation_1_Union = SetOperation_1_Union(
        spark, 
        df_SET_FIELD_ORDER_1, 
        df_SET_FIELD_ORDER_2, 
        df_SET_FIELD_ORDER_3, 
        df_SET_FIELD_ORDER_4, 
        df_SET_FIELD_ORDER_5
    )
    df_SetOperation_1_Union = collectMetrics(
        spark, 
        df_SetOperation_1_Union, 
        "graph", 
        "xKifw-NoXimEOSoY6rBDY$$2DHwRrQJVUoIg5RoTtBRa", 
        "LLCYFN1HuOWzuV3az61i9$$nU1QMY1wYgK_Kngg5TvUV"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_INV_ATL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_INV_ATL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
