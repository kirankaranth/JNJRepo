from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_tai.config.ConfigStore import *
from sap_01_md_sls_ordr_tai.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SLS_ORDR_DOC_ID_VBUK", trim(col("VBELN")))\
        .withColumn("CR_CHK_TOT_STS_CD", trim(col("CMGST")))\
        .withColumn("REJ_TOT_STS_CD", trim(col("ABSTK")))\
        .withColumn("CNFRM_STS_CD", trim(col("BESTK")))\
        .withColumn("PSTNG_BILL_STS_CD", trim(col("BUCHK")))\
        .withColumn("INTCO_BILL_TOT_STS_CD", trim(col("FKIVK")))\
        .withColumn("ORDR_BILL_STS_CD", trim(col("FKSAK")))\
        .withColumn("BILL_STS_CD", trim(col("FKSTK")))\
        .withColumn("PRCSG_TOT_STS_CD", trim(col("GBSTK")))\
        .withColumn("PICK_CNFRM_STS_CD", trim(col("KOQUK")))\
        .withColumn("PICK_TOT_STS_CD", trim(col("KOSTK")))\
        .withColumn("DELV_STS_CD", trim(col("LFSTK")))\
        .withColumn("DELV_TOT_STS_CD", trim(col("LFGSK")))\
        .withColumn("WM_TOT_STS_CD", trim(col("LVSTK")))\
        .withColumn("PACK_TOT_STS_CD", trim(col("PKSTK")))\
        .withColumn("INVC_LIST_STS_CD", trim(col("RELIK")))\
        .withColumn("REF_DOC_TOT_STS_CD", trim(col("RFGSK")))\
        .withColumn("REF_DOC_STS_CD", trim(col("RFSTK")))\
        .withColumn("TRNSP_PLAN_STS_CD", trim(col("TRSTA")))\
        .withColumn("ICMPT_TOT_STS_CD", trim(col("UVALS")))\
        .withColumn("BILL_ICMPT_TOT_STS_CD", trim(col("UVFAS")))\
        .withColumn("PACKICMPT_STS_CD", trim(col("UVPAK")))\
        .withColumn("PACKICMPT_TOT_STS_CD", trim(col("UVPAS")))\
        .withColumn("PICKICMPT_STS_CD", trim(col("UVPIK")))\
        .withColumn("PICKICMPT_TOT_STS_CD", trim(col("UVPIS")))\
        .withColumn("PRCICMPT_STS_CD", trim(col("UVPRS")))\
        .withColumn("DELVICMPT_TOT_STS_CD", trim(col("UVVLS")))\
        .withColumn("GMICMPT_TOT_STS_CD", trim(col("UVWAS")))\
        .withColumn("GM_TOT_STS_CD", trim(col("WBSTK")))\
        .withColumn("DELV_BLK_STS_CD", trim(col("LSSTK")))\
        .withColumn("RESV_CD", trim(col("CMPS1")))\
        .withColumn("OVRL_HDR_CD", trim(col("UVALL")))\
        .withColumn("BILL_BLK_STS_CD", trim(col("FSSTK")))\
        .withColumn("OVRL_BLK_STS_CD", trim(col("SPSTG")))
