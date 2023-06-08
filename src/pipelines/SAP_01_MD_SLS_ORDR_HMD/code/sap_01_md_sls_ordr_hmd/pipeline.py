from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_sls_ordr_hmd.config.ConfigStore import *
from sap_01_md_sls_ordr_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_sls_ordr_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_T001 = SAP_T001(spark)
    df_SAP_T001 = collectMetrics(
        spark, 
        df_SAP_T001, 
        "graph", 
        "JLs07ycsnoIWh5NGGOqe-$$Ps4N1775X2HZlQfz7rrQp", 
        "7kGhPW4DDmgCf_r_q07XI$$5UzBJdENGl123GMPF_Bkz"
    )
    df_MANDT_FILTER_T001 = MANDT_FILTER_T001(spark, df_SAP_T001)
    LU_SAP_T001(spark, df_MANDT_FILTER_T001)
    df_SAP_TVTWT = SAP_TVTWT(spark)
    df_SAP_TVTWT = collectMetrics(
        spark, 
        df_SAP_TVTWT, 
        "graph", 
        "StQ9R00zX6gbGy5UlgA91$$W1YSsDEEWW_1Kc8JHQjUW", 
        "ED2bww86b91VzunkzE14c$$_WMt7rVtyzV5nWDLBLUia"
    )
    df_MANDT_FILTER_TVTWT = MANDT_FILTER_TVTWT(spark, df_SAP_TVTWT)
    LU_SAP_TVTWT(spark, df_MANDT_FILTER_TVTWT)
    df_SAP_TVSBT = SAP_TVSBT(spark)
    df_SAP_TVSBT = collectMetrics(
        spark, 
        df_SAP_TVSBT, 
        "graph", 
        "wEQYRztY8QnPxQ6CpKeqd$$Xgac_rvzb-F7Nk9ClSCyc", 
        "65ja08t2lZ6KiiMshssJR$$6jnU4Liwyi0MZkDqeXbfB"
    )
    df_MANDT_FILTER_TVSBT = MANDT_FILTER_TVSBT(spark, df_SAP_TVSBT)
    LU_SAP_TVSBT(spark, df_MANDT_FILTER_TVSBT)
    df_SAP_TVFST = SAP_TVFST(spark)
    df_SAP_TVFST = collectMetrics(
        spark, 
        df_SAP_TVFST, 
        "graph", 
        "GopZGZ-yy4OxZZ0LVwzH0$$eOEDg8LIwSoKRFpmYf6vo", 
        "Yn-eMGQmvguevm7sIuC0f$$vQiQ2OMcVV6NHEG9JBfxT"
    )
    df_MANDT_FILTER_TVFST = MANDT_FILTER_TVFST(spark, df_SAP_TVFST)
    LU_SAP_TVFST(spark, df_MANDT_FILTER_TVFST)
    df_SAP_TVAKT = SAP_TVAKT(spark)
    df_SAP_TVAKT = collectMetrics(
        spark, 
        df_SAP_TVAKT, 
        "graph", 
        "CwHpcpvwWfFyDk_l_ZBUu$$7PwJaRcVvtWJn_ljcsSV3", 
        "G4Ss9KVVddwUgRu6hbRlX$$NCE5YWWllbuE-ZmTwfxl-"
    )
    df_MANDT_FILTER_TVAKT = MANDT_FILTER_TVAKT(spark, df_SAP_TVAKT)
    LU_SAP_TVAKT(spark, df_MANDT_FILTER_TVAKT)
    df_SAP_TVKOT = SAP_TVKOT(spark)
    df_SAP_TVKOT = collectMetrics(
        spark, 
        df_SAP_TVKOT, 
        "graph", 
        "A0_moA2g1cQbBv3fWQuyp$$7cUuVwNR8AG-31yeaPutl", 
        "Nuqc1Igy8kwKihTVimJlK$$_gF-IEE-4-fN9PdtdPceG"
    )
    df_MANDT_FILTER_TVKOT = MANDT_FILTER_TVKOT(spark, df_SAP_TVKOT)
    LU_SAP_TVKOT(spark, df_MANDT_FILTER_TVKOT)
    df_SAP_TVAU = SAP_TVAU(spark)
    df_SAP_TVAU = collectMetrics(
        spark, 
        df_SAP_TVAU, 
        "graph", 
        "Fp_mqI2zH97GKHtdB2GRA$$icgUD7HHgRkOqWZQrvzOA", 
        "da1Y9TShl0rfq3hpVqu3E$$GlQ1O1K1w8Q9phqiv0UCb"
    )
    df_MANDT_FILTER_TVAU = MANDT_FILTER_TVAU(spark, df_SAP_TVAU)
    LU_SAP_TVAU(spark, df_MANDT_FILTER_TVAU)
    df_SAP_T176T = SAP_T176T(spark)
    df_SAP_T176T = collectMetrics(
        spark, 
        df_SAP_T176T, 
        "graph", 
        "RUJcxdA6uCcj7MjqVmOnA$$uGNlJ0OyD3etwSfCM8tk0", 
        "gg6gVZoISKLN1y68m8DAF$$b_SqXNA5UmrxyT9sG9deA"
    )
    df_MANDT_FILTER_T176T = MANDT_FILTER_T176T(spark, df_SAP_T176T)
    LU_SAP_T176T(spark, df_MANDT_FILTER_T176T)
    df_SAP_TVLST = SAP_TVLST(spark)
    df_SAP_TVLST = collectMetrics(
        spark, 
        df_SAP_TVLST, 
        "graph", 
        "ftAmq5H5Ek-3Wi18IShFs$$FzA3WpWkMVTDdhSkW_v9Z", 
        "_HiMu4DuUedWcqvrWx_7C$$fadSJ5mOvyzI_ETb82FIe"
    )
    df_MANDT_FILTER_TVLST = MANDT_FILTER_TVLST(spark, df_SAP_TVLST)
    LU_SAP_TVLST(spark, df_MANDT_FILTER_TVLST)
    df_SAP_TSPAT = SAP_TSPAT(spark)
    df_SAP_TSPAT = collectMetrics(
        spark, 
        df_SAP_TSPAT, 
        "graph", 
        "kAfSGe_8JuQOy7eZdp1DQ$$nxvUHGwsnlJTIfSnHJc5l", 
        "knvKmhvK8A-GBaGNLnj6e$$L_NJ-BQFKGH4Sd5jztEOz"
    )
    df_MANDT_FILTER_TSPAT = MANDT_FILTER_TSPAT(spark, df_SAP_TSPAT)
    LU_SAP_TSPAT(spark, df_MANDT_FILTER_TSPAT)
    df_SAP_TVAUT = SAP_TVAUT(spark)
    df_SAP_TVAUT = collectMetrics(
        spark, 
        df_SAP_TVAUT, 
        "graph", 
        "anSdOaas_5__Yh2lzJ-x6$$BV0NU-18GF-qPPVDaO5Ie", 
        "3fkuKUq3ufrvEGPurBcdq$$GkYvWAptzLF2tBaVRSQP1"
    )
    df_MANDT_FILTER_TVAUT = MANDT_FILTER_TVAUT(spark, df_SAP_TVAUT)
    LU_SAP_TVAUT(spark, df_MANDT_FILTER_TVAUT)
    df_SAP_VBAK = SAP_VBAK(spark)
    df_SAP_VBAK = collectMetrics(
        spark, 
        df_SAP_VBAK, 
        "graph", 
        "oai-k6rsM3DaXL8zlH0gP$$4ma6w2BOf6zMU4wjWX7TZ", 
        "PXwL7I3W4R6S5dTxhzhwA$$GTHn8zlGc59X8m7Dg0wJi"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VBAK)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "fnuWA3iEP8yj0Wpmr7x7w$$RlB3rFyhHr8kKTVsFk6Jj", 
        "mtvb5N4jIUORTT5rEKyuI$$G_MoZoeS_MwazxHSJUA4l"
    )
    df_GET_DUP = GET_DUP(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUP_FILTER = DUP_FILTER(spark, df_GET_DUP)
    df_DUP_FILTER = collectMetrics(
        spark, 
        df_DUP_FILTER, 
        "graph", 
        "6Ox2NQn66uCJXu0tsLuTT$$OIhLh__wvgzXE0AXUcTi1", 
        "-ZvjtQMEZsEHoq2pY6-mY$$AC5SgPju82x-bO3RoNp85"
    )
    df_DUP_FILTER.cache().count()
    df_DUP_FILTER.unpersist()
    MD_SLS_ORDR(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_SLS_ORDR_HMD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_SLS_ORDR_HMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
