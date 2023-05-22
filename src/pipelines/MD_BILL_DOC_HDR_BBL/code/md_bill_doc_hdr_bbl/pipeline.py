from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from md_bill_doc_hdr_bbl.config.ConfigStore import *
from md_bill_doc_hdr_bbl.udfs.UDFs import *
from prophecy.utils import *
from md_bill_doc_hdr_bbl.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_TVKOT = SAP_TVKOT(spark)
    df_SAP_TVKOT = collectMetrics(
        spark, 
        df_SAP_TVKOT, 
        "graph", 
        "WjoLIbiZu3ol3FK386xCE$$rGRtZbn2QVAvAUnzMzJme", 
        "ASWvg_Yg6bVSC_4KkkCP_$$VvULMbt5spAYVvk81fAQe"
    )
    df_MANDT_FILTER_TVKOT = MANDT_FILTER_TVKOT(spark, df_SAP_TVKOT)
    df_MANDT_FILTER_TVKOT = collectMetrics(
        spark, 
        df_MANDT_FILTER_TVKOT, 
        "graph", 
        "gns-x00h4uj8kJSaOgROh$$Bsaa3Gi_nOQd1ibddytHI", 
        "rzNsBaE0ME8NEaNpBr9lP$$ICSY3CicrolRNq7aM6Zmx"
    )
    LU_SAP_TVKOT(spark, df_MANDT_FILTER_TVKOT)
    df_SAP_TVFKT = SAP_TVFKT(spark)
    df_SAP_TVFKT = collectMetrics(
        spark, 
        df_SAP_TVFKT, 
        "graph", 
        "7aZgH4cXYOPuP980DxtlD$$c36XcDT64UJYhzm0zZb25", 
        "atxU07d3Wbdlwl1Z-qn1r$$rirtkNhjXF6hSfewqvx21"
    )
    df_SAP_TVTWT = SAP_TVTWT(spark)
    df_SAP_TVTWT = collectMetrics(
        spark, 
        df_SAP_TVTWT, 
        "graph", 
        "y8fApq8JNLqGlbDQc0848$$gh6VJOGAo5dfQzmkvGHAm", 
        "d9SRIyWjocBcgnL7lE09K$$RZQZSDvUrTW9m2Lz13y7_"
    )
    df_MANDT_FILTER_TVTWT = MANDT_FILTER_TVTWT(spark, df_SAP_TVTWT)
    df_MANDT_FILTER_TVTWT = collectMetrics(
        spark, 
        df_MANDT_FILTER_TVTWT, 
        "graph", 
        "eaXysMvyhvULofGGi94TM$$iyCfXZWMEk7-BsOc1gsVU", 
        "7dGsVDgiBbb17uNuMOyeO$$uup30YJ4a8fp2H6keenf6"
    )
    df_MANDT_FILTER_TVFKT = MANDT_FILTER_TVFKT(spark, df_SAP_TVFKT)
    df_MANDT_FILTER_TVFKT = collectMetrics(
        spark, 
        df_MANDT_FILTER_TVFKT, 
        "graph", 
        "lKOl85wxlSbUkte_bs3zr$$UyDJcpyNkUDw6FYZUFxkQ", 
        "yILpbr1GzySj7xKw3QIiY$$QNsZOxmi7uVsxU-k0sMCN"
    )
    LU_SAP_TVFKT(spark, df_MANDT_FILTER_TVFKT)
    df_SAP_TSPAT = SAP_TSPAT(spark)
    df_SAP_TSPAT = collectMetrics(
        spark, 
        df_SAP_TSPAT, 
        "graph", 
        "eOh29uSQFIsmxxhdtA3c4$$7HvK4VJaAbaqdh2n4abj7", 
        "U_bgi61MGiiHryz2KuiFb$$N5tdKGjI5YndJcHjnw3ey"
    )
    df_MANDT_FILTER_TSPAT = MANDT_FILTER_TSPAT(spark, df_SAP_TSPAT)
    df_MANDT_FILTER_TSPAT = collectMetrics(
        spark, 
        df_MANDT_FILTER_TSPAT, 
        "graph", 
        "StQbO7iS-VLl-XnpfwET-$$DFey_ozyS8oGBplQUKQld", 
        "5nB4T_yd3LtUIeKy5dpI_$$q8cK2xQtLqYDkWnTS_RsR"
    )
    LU_SAP_TSPAT(spark, df_MANDT_FILTER_TSPAT)
    LU_SAP_TVTWT(spark, df_MANDT_FILTER_TVTWT)
    df_SAP_VBRK = SAP_VBRK(spark)
    df_SAP_VBRK = collectMetrics(
        spark, 
        df_SAP_VBRK, 
        "graph", 
        "LLApbaM__ftWOps0u1op9$$EOGYa6LH7CXEiDXmnObdn", 
        "fF9CxUY7wdLFpy-1fHf3J$$GLvEmsTFfkDXBhLbIC7qM"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_VBRK)
    df_MANDT_FILTER = collectMetrics(
        spark, 
        df_MANDT_FILTER, 
        "graph", 
        "K51pKUsFc8Va6d8he8XTF$$oVNHjkgb9cWiohlAWCn3D", 
        "aVbKvgaxR9MkOcO1iMS74$$Znom8l5AOOCnmrBaqaYJo"
    )
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_NEW_FIELDS_RENAME_FORMAT = collectMetrics(
        spark, 
        df_NEW_FIELDS_RENAME_FORMAT, 
        "graph", 
        "Sfz2ypx5RFwSOfjsu2vt4$$arK71I3EleBs1MZzBmIEM", 
        "OleHEXhYpESTxYDWfqUPO$$4yEEYpUBNCFjl4fPYaQ3s"
    )
    df_SET_FIELD_ORDER_FORMAT = SET_FIELD_ORDER_FORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_FORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_FORMAT, 
        "graph", 
        "EbdOHRUfGkkfL8l6XCfXF$$olQuOop_OA69BS-Y-GVOQ", 
        "f6Ijtk2iSh3QABhayQJb4$$t_g1w0vL40Ygzi3emDIQm"
    )
    MD_BILL_DOC_HDR(spark, df_SET_FIELD_ORDER_FORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_FORMAT)
    df_DUPLICATE_CHECK = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK, 
        "graph", 
        "-L74TpUxTNGIpUUhwcBsl$$nGQziur5IhIGLuvAAAzn2", 
        "D0UtP9iC5VkoxGL79fNUI$$wbpYKMaNoEVLO5kVYnU8Z"
    )
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "szkaikeSehIZw-Uc_pEPn$$Win0abo2BtqGYHXLzKdJG", 
        "AaQelDl3pqzx5SPUVb_IT$$aITk5Se2LdSOF8cSePohq"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_BILL_DOC_HDR_BBL")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_BILL_DOC_HDR_BBL")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
