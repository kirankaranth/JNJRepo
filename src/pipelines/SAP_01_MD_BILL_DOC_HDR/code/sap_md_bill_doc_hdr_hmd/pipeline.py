from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_bill_doc_hdr_hmd.config.ConfigStore import *
from sap_md_bill_doc_hdr_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_bill_doc_hdr_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_VBRK = DS_SAP_01_VBRK(spark)
    df_DS_SAP_01_VBRK = collectMetrics(
        spark, 
        df_DS_SAP_01_VBRK, 
        "graph", 
        "ZkhBFp2QdFgX6ukfOgXLw$$FnnuP_Lx7_Qgrj6W9o5XW", 
        "enrOKVhTmV3EQHGKZLf70$$dAxwmYiRye6v9URwZ3Uab"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_01_VBRK)
    df_MANDT_FILTER = collectMetrics(
        spark, 
        df_MANDT_FILTER, 
        "graph", 
        "deXvkRShBftNgQerXTRbl$$BOxY3jUjvkEYtA6tHcqFH", 
        "ogoK3AsMdRcHyRM2yUJYo$$huGt4oxK2EafOaulThExc"
    )
    df_DS_SAP_01_TSPAT = DS_SAP_01_TSPAT(spark)
    df_DS_SAP_01_TSPAT = collectMetrics(
        spark, 
        df_DS_SAP_01_TSPAT, 
        "graph", 
        "ULfLUzJ36w4WlYK3qfNNv$$vhTHpqxMAQHTt4fAQ9p9u", 
        "QnZCLz-VPfrr57NyptqOr$$jVVM9C5DN0UdyS1m6gSug"
    )
    df_MANDT_FILTER_TSPAT = MANDT_FILTER_TSPAT(spark, df_DS_SAP_01_TSPAT)
    df_MANDT_FILTER_TSPAT = collectMetrics(
        spark, 
        df_MANDT_FILTER_TSPAT, 
        "graph", 
        "TKXEvyiaGK1N0vkTXZBh4$$VxkETNAAkLQj_cJAxJVfX", 
        "XB3jvpgjsqqtVn01qWRKz$$5ovjO2hLi4rMTwLEFo5jZ"
    )
    df_DS_SAP_01_TVFKT = DS_SAP_01_TVFKT(spark)
    df_DS_SAP_01_TVFKT = collectMetrics(
        spark, 
        df_DS_SAP_01_TVFKT, 
        "graph", 
        "aii1hzZFb_RmA_O1UFRqk$$H8nMlUTAO8ovdLtJzAV3w", 
        "QwSKvORCybfMl9_IWt-V8$$Ol-Ni2V-Zp5yI0nEWPb-t"
    )
    df_MANDT_FILTER_TVFKT = MANDT_FILTER_TVFKT(spark, df_DS_SAP_01_TVFKT)
    df_MANDT_FILTER_TVFKT = collectMetrics(
        spark, 
        df_MANDT_FILTER_TVFKT, 
        "graph", 
        "lMYaFuGD9wCMdaG1vzfGN$$ge4HzPoOQuowLJZfZcSRm", 
        "ZJ5pUvdmjpOeY6SEVbdJp$$EGcNalp-izRysDPbs7Te8"
    )
    df_DS_SAP_01_TVKOT = DS_SAP_01_TVKOT(spark)
    df_DS_SAP_01_TVKOT = collectMetrics(
        spark, 
        df_DS_SAP_01_TVKOT, 
        "graph", 
        "gvFo1r8ljxBPykwKJ_CeU$$zj57r8Ox4B1G10vcnkYhK", 
        "krsMj76rPl9HgWKijPNwG$$VL2HkRr8oeNNxWeh01WRx"
    )
    df_MANDT_FILTER_TVKOT = MANDT_FILTER_TVKOT(spark, df_DS_SAP_01_TVKOT)
    df_MANDT_FILTER_TVKOT = collectMetrics(
        spark, 
        df_MANDT_FILTER_TVKOT, 
        "graph", 
        "Xp8siEHgR3IUtx6hm2Xhy$$tUIuD7S4L6lz5OWcimhz1", 
        "43Kzf0nC4iZxfkM6HZngo$$pmQ1cFGNv-J9PpF6hQDyy"
    )
    df_DS_SAP_01_TVTWT = DS_SAP_01_TVTWT(spark)
    df_DS_SAP_01_TVTWT = collectMetrics(
        spark, 
        df_DS_SAP_01_TVTWT, 
        "graph", 
        "yiwKc2qmx4MdCPrk6HOmz$$a3mQ_xMD-Swz1eC9hgM1Z", 
        "dV9pjbT4TpbpmlRKboj7R$$EBwTWvXaVskVaKyQEWARP"
    )
    df_MANDT_FILTER_TVTWT = MANDT_FILTER_TVTWT(spark, df_DS_SAP_01_TVTWT)
    df_MANDT_FILTER_TVTWT = collectMetrics(
        spark, 
        df_MANDT_FILTER_TVTWT, 
        "graph", 
        "JZXDvDbS9kNKa1NRZeekV$$5XlZ-goj_GMIED0j7jekQ", 
        "LHTkNegAbW1b0dokiOa-1$$0Cs-IqeKbAQNnUASwO7UI"
    )
    df_Join_1 = Join_1(
        spark, 
        df_MANDT_FILTER, 
        df_MANDT_FILTER_TSPAT, 
        df_MANDT_FILTER_TVFKT, 
        df_MANDT_FILTER_TVKOT, 
        df_MANDT_FILTER_TVTWT
    )
    df_Join_1 = collectMetrics(
        spark, 
        df_Join_1, 
        "graph", 
        "VvaO9fWnm5o7Xc10hN8V1$$jsQHHYOVMz6MfW0w3a9kZ", 
        "tcptYmG7T_lpsHsrnImZE$$Hj5-AvvIlDwsC0T6YEKdu"
    )
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_NEW_FIELDS_RENAME_FORMAT = collectMetrics(
        spark, 
        df_NEW_FIELDS_RENAME_FORMAT, 
        "graph", 
        "0YpXL_YCj6ptl63tdGeLb$$_tdhUUjXu8PsuhLn1mc90", 
        "5nDoKbR2pTZlqc7MKtjsi$$pR7P_POtzlrhyIa-0RMrb"
    )
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "RkuXkVDYnBRtP3TUQAPw7$$4obr3llnjHUcwW-OKU_YU", 
        "lO0LGUOBjBH2sPU6SIEOS$$nSFF8bxogn2Oq2WBsCF4Y"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK, 
        "graph", 
        "gGnEyWOJxuI_XmM_mqhdG$$vG1WfIRsd_EcUF1mj4krG", 
        "rNkgp8YJsMSGrdsYR7kzj$$gbyr5XHKU6dwyflOCJaFw"
    )
    MD_BILL_DOC_HDR(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "DRlxCG47iy9hrdDb9STjj$$KBV7fRVWSwIR3Dvvh6MtG", 
        "_yNmeNALDv7D6_3s3Q12t$$I5U0d41gJb_ZaOJIyrJfr"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_BILL_DOC_HDR")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_BILL_DOC_HDR")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
