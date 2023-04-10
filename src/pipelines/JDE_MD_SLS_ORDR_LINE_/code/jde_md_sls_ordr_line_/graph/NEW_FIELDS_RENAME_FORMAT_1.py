from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_line_.config.ConfigStore import *
from jde_md_sls_ordr_line_.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("COMPANY_CD", col("SDKCOO"))\
        .withColumn("SLS_ORDR_DOC_ID", col("SDDOCO"))\
        .withColumn("SLS_ORDR_LINE_NBR", col("SDLNID"))\
        .withColumn("SLS_ORDR_TYPE_CD", col("SDDCTO"))\
        .withColumn("CNFRM_QTY", col("SDSOQS").cast(DecimalType(18, 4)))\
        .withColumn("SLS_UNIT_ORDR_QTY", col("SDUORG").cast(DecimalType(18, 4)))\
        .withColumn("SLOC_CD", trim(col("SDLOCN")))\
        .withColumn("MATL_NUM", trim(col("SDLITM")))\
        .withColumn("NET_VAL_AMT", col("SDAEXP").cast(DecimalType(18, 4)))\
        .withColumn("RTE_ID", trim(col("SDROUT")))\
        .withColumn("SLS_UOM_CD", trim(col("SDUOM")))\
        .withColumn("SLS_ORDR_CRNCY_CD", trim(col("SDCRCD")))\
        .withColumn("PLNT_CD", trim(col("SDMCU")))\
        .withColumn("INTNL_COM_CD", trim(col("SDPTC")))\
        .withColumn("CNFRM_STS_CD", trim(col("SDCOMM")))\
        .withColumn("PRCSG_TOT_STS_CD", trim(col("SDNXTR")))\
        .withColumn("PICK_STS_CD", trim(col("SDPSN")))\
        .withColumn("DELV_STS_CD", trim(col("SDDELN")))\
        .withColumn(
          "CR_DTTM",
          to_timestamp(
            date_add(
              substring((col("SDTRDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SDTRDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("BASE_UOM_CD", trim(col("SDUOM1")))\
        .withColumn(
          "AVAIL_TO_PROM_DTTM",
          to_timestamp(
            date_add(
              substring((col("SDTRDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SDTRDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("GRS_WT_MEAS", col("SDGRWT").cast(DecimalType(18, 4)))\
        .withColumn("WT_UOM_CD", trim(col("SDWTUM")))\
        .withColumn("NET_WT_MEAS", col("SDITWT").cast(DecimalType(18, 4)))\
        .withColumn("VOL_UOM_CD", trim(col("SDVLUM")))\
        .withColumn("VOL_MEAS   ", col("SDITVL").cast(DecimalType(18, 4)))\
        .withColumn("COST_IN_DOC_CRNCY", col("SDECST").cast(DecimalType(18, 4)))\
        .withColumn("BTCH_NUM", trim(col("SDLOTN")))\
        .withColumn(
          "ORIG_RQST_DELV_DTTM",
          to_timestamp(
            date_add(
              substring((col("SDDRQJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SDDRQJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "CHG_DTTM",
          to_timestamp(
            date_add(
              substring((col("SDUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SDUPMJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("DR_CR_IN", trim(col("SDCADC")))\
        .withColumn("CRT_BY_NM", trim(col("SDTORG")))\
        .withColumn("MATL_GRP_1", trim(col("SDSRP1")))\
        .withColumn("MATL_GRP_2_MVGR2", trim(col("SDSRP2")))\
        .withColumn("MATL_GRP_3", trim(col("SDSRP3")))\
        .withColumn("MATL_GRP_4", trim(col("SDSRP4")))\
        .withColumn("MATL_GRP_5", trim(col("SDSRP5")))\
        .withColumn("OVRD_PRC_RES_CD", trim(col("SDPROV")))\
        .withColumn("TAX_CLSN_FOR_MATL_1", trim(col("SDTXA1")))\
        .withColumn("TAX_CLSN_FOR_MATL_2", trim(col("SDEXR1")))\
        .withColumn("ACCT_ASGNMT_GRP", trim(col("SDGLC")))\
        .withColumn("STATS_EXCH_RT", col("SDCRR").cast(DecimalType(18, 4)))\
        .withColumn(
          "BILL_DTTM",
          to_timestamp(
            date_add(
              substring((col("SDIVD") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SDIVD"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("SHIP_TO_PRTY", trim(col("SDSHAN")))\
        .withColumn("BLLT_PRTY", trim(col("SDAN8")))\
        .withColumn(
          "CNCL_DTTM",
          to_timestamp(
            date_add(
              substring((col("SDCNDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SDCNDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SLS_ORDR_LINE_NBR', SLS_ORDR_LINE_NBR, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SLS_ORDR_LINE_NBR', SLS_ORDR_LINE_NBR, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
