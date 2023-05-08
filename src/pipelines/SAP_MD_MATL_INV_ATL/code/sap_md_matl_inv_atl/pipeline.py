from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_inv_atl.config.ConfigStore import *
from sap_md_matl_inv_atl.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_inv_atl.graph import *

def pipeline(spark: SparkSession) -> None:
    Lookup_1_MARA(spark)
    df_DS_SAP_0000002_MCHB = DS_SAP_0000002_MCHB(spark)
    df_DS_SAP_0000002_MCHB = collectMetrics(
        spark, 
        df_DS_SAP_0000002_MCHB, 
        "graph", 
        "YaM1fn39DJoHvnTgoW8Ds$$BNOGRfKYh-LVTCSxrMUPw", 
        "XZsf4J9LDmNZ3SkZGr9ht$$ZGTsBoKFUyxjPf9fIooxC"
    )
    df_DS_SAP_0000005_MSSL = DS_SAP_0000005_MSSL(spark)
    df_DS_SAP_0000005_MSSL = collectMetrics(
        spark, 
        df_DS_SAP_0000005_MSSL, 
        "graph", 
        "b1hWFGoZwCugRsi9bNbo2$$WQz4bUADeUOBCsQSVDWLA", 
        "ACLiT7HycPUHxgS9h73z5$$4gPZAc7x7o3JQI1YnXfpa"
    )
    df_MANDT_5 = MANDT_5(spark, df_DS_SAP_0000005_MSSL)
    df_SELECT_MSSL = SELECT_MSSL(spark, df_MANDT_5)
    df_SELECT_MSSL = collectMetrics(
        spark, 
        df_SELECT_MSSL, 
        "graph", 
        "exKYGwFOvd_sVIUyQ5R9U$$pCKG18pVTFT4_h1yMPO5B", 
        "qwRowAdJ26_jNGHi16BHo$$QspaObCKlpv5kTi5ByOBj"
    )
    df_SELECT_MSSL.cache().count()
    df_SELECT_MSSL.unpersist()
    df_DS_SAP_0000004_MSLB = DS_SAP_0000004_MSLB(spark)
    df_DS_SAP_0000004_MSLB = collectMetrics(
        spark, 
        df_DS_SAP_0000004_MSLB, 
        "graph", 
        "oobBQwuiBj7ore1GoFcDm$$2f4Bbd1WMA1CZ9rIMLElD", 
        "8nL-3lUELkgn4haw_OD2h$$iFm3V3TPeEyeJWmb7fan4"
    )
    df_MANDT_4 = MANDT_4(spark, df_DS_SAP_0000004_MSLB)
    df_SchemaTransform_2_MCHB = SchemaTransform_2_MCHB(spark)
    df_SET_FIELD_ORDER_2 = SET_FIELD_ORDER_2(spark, df_SchemaTransform_2_MCHB)
    df_MANDT_2 = MANDT_2(spark, df_DS_SAP_0000002_MCHB)
    df_Select_MCHB = Select_MCHB(spark, df_MANDT_2)
    df_Select_MCHB = collectMetrics(
        spark, 
        df_Select_MCHB, 
        "graph", 
        "5dvQ2TzbtdkaTHiIfSJcd$$3uqr72vWHuk-3XibRC5OG", 
        "RGbf_zbObduQDfvFeK7wg$$TeVVxYz0pPmjBI_0okVQS"
    )
    df_Select_MCHB.cache().count()
    df_Select_MCHB.unpersist()
    df_SchemaTransform_3_MKOL = SchemaTransform_3_MKOL(spark)
    df_SET_FIELD_ORDER_3 = SET_FIELD_ORDER_3(spark, df_SchemaTransform_3_MKOL)
    df_SchemaTransform_5_MSSL = SchemaTransform_5_MSSL(spark)
    df_SET_FIELD_ORDER_5 = SET_FIELD_ORDER_5(spark, df_SchemaTransform_5_MSSL)
    df_SchemaTransform_4_MSLB = SchemaTransform_4_MSLB(spark)
    df_SchemaTransform_1_MARD = SchemaTransform_1_MARD(spark)
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
    df_DS_SAP_0000003_MKOL = DS_SAP_0000003_MKOL(spark)
    df_DS_SAP_0000003_MKOL = collectMetrics(
        spark, 
        df_DS_SAP_0000003_MKOL, 
        "graph", 
        "QtliVpMYiwU9jss57BUIB$$0pqUECXCWnZO69-LTkizA", 
        "b_x4_YkQB3KV-r_kyQWZG$$dsxZOaYupJMRTut5U0pae"
    )
    df_MANDT_3 = MANDT_3(spark, df_DS_SAP_0000003_MKOL)
    df_Select_MKOL = Select_MKOL(spark, df_MANDT_3)
    df_Select_MKOL = collectMetrics(
        spark, 
        df_Select_MKOL, 
        "graph", 
        "jlOBPCB0sgdxKwJQd7lxC$$n43JJqT6nwTFshL8SUmi7", 
        "QCoj9ze5FLDyRJPmCpAGA$$q0KBEJGCcNB34_-0VvZCM"
    )
    df_Select_MKOL.cache().count()
    df_Select_MKOL.unpersist()
    df_DS_SAP_0000006_MARA = DS_SAP_0000006_MARA(spark)
    df_DS_SAP_0000006_MARA = collectMetrics(
        spark, 
        df_DS_SAP_0000006_MARA, 
        "graph", 
        "IJ5NuzifNlRHw1_S3acvn$$XZiimepA2pHCz3CG1qyUi", 
        "MybdmUVzs15xSdR_1-D_J$$odQZABeQQyezVnDG0iPmv"
    )
    df_MANDT_6 = MANDT_6(spark, df_DS_SAP_0000006_MARA)
    MD_MATL_INV(spark, df_SetOperation_1_Union)
    df_DS_SAP_0000001_MARD = DS_SAP_0000001_MARD(spark)
    df_DS_SAP_0000001_MARD = collectMetrics(
        spark, 
        df_DS_SAP_0000001_MARD, 
        "graph", 
        "N3kaCSsahMsUWknQ9M67x$$i8bk6KYGJ_gbIcvNKTL3w", 
        "zbp2a-qKqL5YcabsPqTSS$$-_mucTqkssRXA3cW606RA"
    )
    df_MANDT_1 = MANDT_1(spark, df_DS_SAP_0000001_MARD)
    df_Select_MARD_Columns = Select_MARD_Columns(spark, df_MANDT_1)
    df_Select_MARD_Columns = collectMetrics(
        spark, 
        df_Select_MARD_Columns, 
        "graph", 
        "FuZn1W-qoeHIkm1TG7SBa$$mATcsgtaPyEERcBA9GVp_", 
        "18jFw_-0sHU2xRvLY3yCH$$dmVYJebdw6zsbYAqEXKqJ"
    )
    df_Select_MARD_Columns.cache().count()
    df_Select_MARD_Columns.unpersist()
    df_SELECT_MARA = SELECT_MARA(spark, df_MANDT_6)
    df_SELECT_MARA = collectMetrics(
        spark, 
        df_SELECT_MARA, 
        "graph", 
        "w3ys13DyozKiR5ptDEo8t$$wt1wG8sSqdAJIzxja4FZy", 
        "nqHm-Y1OlkMWzi8eQfmmo$$KJ3lbojCTAsTzpjxrD4BH"
    )
    df_SELECT_MARA.cache().count()
    df_SELECT_MARA.unpersist()
    df_SELECT_MSLB = SELECT_MSLB(spark, df_MANDT_4)
    df_SELECT_MSLB = collectMetrics(
        spark, 
        df_SELECT_MSLB, 
        "graph", 
        "RCjM0fZ4xSWaDuqwy5Ps3$$QiRopmqNgLedN-B18l0No", 
        "RdaHbGjUTrbFVmoOVoAd3$$H501nEhc6tv0HRizGe5AB"
    )
    df_SELECT_MSLB.cache().count()
    df_SELECT_MSLB.unpersist()

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
