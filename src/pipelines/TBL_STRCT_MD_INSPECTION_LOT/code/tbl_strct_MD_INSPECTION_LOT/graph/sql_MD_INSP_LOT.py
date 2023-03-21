from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_INSPECTION_LOT.config.ConfigStore import *
from tbl_strct_MD_INSPECTION_LOT.udfs.UDFs import *

def sql_MD_INSP_LOT(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as LOT_NUM,\ncast('' as string) as PLNT_CD,\ncast('' as string) as LOT_VERIF_TYPE_CD,\ncast('' as string) as LOT_ORIG_CD,\ncast('' as string) as STS_PRFL,\ncast('' as string) as LOT_SRC_INSP,\ncast('' as string) as USG_DEC_IND,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as timestamp) as INSP_STRT_DTTM,\ncast('' as timestamp) as INSP_END_DTTM,\ncast('' as string) as MFG_ORDR_NUM,\ncast('' as string) as CSTM_NUM,\ncast('' as string) as VNDR_NUM,\ncast('' as string) as MATL_NUM,\ncast('' as string) as BTCH_NUM,\ncast('' as string) as S_LOC_CD,\ncast('' as string) as PO_DOC_NUM,\ncast('' as string) as PO_DOC_LINE_NBR,\ncast('' as string) as MATL_MVMT_DOC_YR,\ncast('' as string) as MATL_MVMT_DOC_NUM,\ncast('' as string) as MATL_MVMT_DOC_LINE_NBR,\ncast('' as string) as STCK_PLNT_CD,\ncast('' as string) as STCK_S_LOC_CD,\ncast('' as string) as SLS_ORDR_DOC_NUM,\ncast('' as string) as SLS_ORDR_DOC_LINE_NBR,\ncast('' as string) as DEL_DOC_NUM,\ncast('' as string) as DEL_DOC_LINE_NBR,\ncast('' as string) as LOT_DESC,\ncast('' as decimal(18,4)) as INSP_LOT_QTY,\ncast('' as string) as BASE_UOM,\ncast('' as decimal(18,4)) as UNRSTK_USE_QTY,\ncast('' as decimal(18,4)) as SCRAP_QTY,\ncast('' as decimal(18,4)) as SAMPLE_QTY,\ncast('' as decimal(18,4)) as BLOCKED_QTY,\ncast('' as decimal(18,4)) as RESERVE_QTY,\ncast('' as decimal(18,4)) as ANOTHER_MAT_QTY,\ncast('' as decimal(18,4)) as TO_BE_POSTED_QTY,\ncast('' as decimal(18,4)) as ACTL_LOT_QTY,\ncast('' as string) as USG_DCSN_CD,\ncast('' as timestamp) as QC_DCSN_DTTM,\ncast('' as string) as QC_STS_CD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
