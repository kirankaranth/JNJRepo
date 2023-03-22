from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_VENDOR_MASTER.config.ConfigStore import *
from tbl_strct_MD_VENDOR_MASTER.udfs.UDFs import *

def sql_MD_SUP(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SUP_NUM,\ncast('' as string) as SUP_NM1,\ncast('' as string) as SUP_NM2,\ncast('' as string) as SUP_NM3,\ncast('' as string) as SUP_NM4,\ncast('' as string) as SUP_SHRT_NM,\ncast('' as string) as CTRY_CD,\ncast('' as string) as TAX_JURIS_CD,\ncast('' as string) as TRSPN_ZN_CD,\ncast('' as string) as CITY_NM,\ncast('' as string) as DSTRC_NM,\ncast('' as string) as PSTL_BOX_CD,\ncast('' as string) as PSTL_BOX_PSTL_CD_NUM,\ncast('' as string) as PSTL_CD_NUM,\ncast('' as string) as RGN_NM,\ncast('' as string) as ADDR_LINE_1_TXT,\ncast('' as string) as CTRY_SHRT_NM,\ncast('' as string) as ADDR_NUM,\ncast('' as string) as GLN1_NBR,\ncast('' as string) as SGMNT_CD,\ncast('' as timestamp) as CRT_ON_DTTM,\ncast('' as string) as SUP_TYPE_CD,\ncast('' as string) as CUST_NUM,\ncast('' as string) as ALT_PAYEE_NUM,\ncast('' as string) as DEL_IND,\ncast('' as string) as PSTNG_BLK_IND,\ncast('' as string) as PRCH_BLK_IND,\ncast('' as string) as TAX_NUM1,\ncast('' as string) as TAX_NUM2,\ncast('' as string) as ONE_TIME_ACCT_IND,\ncast('' as string) as TRAD_PTNR_CO_CD,\ncast('' as string) as VAT_NUM,\ncast('' as string) as NTRL_PRSN_IND,\ncast('' as string) as PLNT_CD,\ncast('' as string) as PMT_BLK_IND,\ncast('' as string) as STD_CARR_ACCS_CD,\ncast('' as string) as TAX_TYPE_CD,\ncast('' as string) as TAX_NUM_TYPE_CD,\ncast('' as string) as TAX_NUM3,\ncast('' as string) as GLN2_NBR,\ncast('' as string) as TAX_NUM4,\ncast('' as string) as TAX_NUM5,\ncast('' as string) as GLN3_NBR,\ncast('' as string) as BLOK_SUP_IND,\ncast('' as string) as POD_IND,\ncast('' as string) as EXTRNL_MFR_CD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
