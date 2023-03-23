from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_RESOURCE.config.ConfigStore import *
from tbl_strct_MES_MD_RESOURCE.udfs.UDFs import *

def sql_MES_MD_EMP(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as EMP_ID,\ncast('' as BOOLEAN) as ALLW_MLT_SIGN_IN_IND,\ncast('' as BOOLEAN) as ALLW_MLT_TEAMS_IND,\ncast('' as BOOLEAN) as ALLW_OVRD_OF_SESS_VAL_IND,\ncast('' as BOOLEAN) as CN_LGN_IND,\ncast('' as BOOLEAN) as CN_UPDT_STERILIZATION_DT_IND,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CHG_STS_ID,\ncast('' as STRING) as DOC_MGR_PSWRD_TXT,\ncast('' as STRING) as DOC_MGR_USER_NM,\ncast('' as STRING) as DOM_NM,\ncast('' as BOOLEAN) as EDIT_APPR_SHT_IND,\ncast('' as BOOLEAN) as EDIT_LBR_SUP_IND,\ncast('' as STRING) as EML_ADDR_TXT,\ncast('' as STRING) as EMP_DESC,\ncast('' as STRING) as EMP_LOG_IN_INFO_ID,\ncast('' as STRING) as EMP_NM,\ncast('' as STRING) as FULL_NM,\ncast('' as STRING) as HIST_VIEW_ID,\ncast('' as STRING) as ICON_ID,\ncast('' as STRING) as INTNL_ID,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as STRING) as JDE_ADDR_BOOK_REC_VAL,\ncast('' as STRING) as LANG_DICT_ID,\ncast('' as STRING) as MENU_DEF_ID,\ncast('' as BOOLEAN) as MDLER_ACCS_IND,\ncast('' as STRING) as NOMENCULATURE_ID,\ncast('' as STRING) as NOTES_TXT,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as PORTL_MENU_DEF_ID,\ncast('' as STRING) as PORTL_MOBL_MENU_DEF_ID,\ncast('' as STRING) as PRMRY_ORG_ID,\ncast('' as STRING) as SESS_VAL_ID,\ncast('' as STRING) as TERMINOLOGY_DICT_ID,\ncast('' as STRING) as TRAIN_PLAN_ID,\ncast('' as STRING) as UI_PORTL_PRFL_ID,\ncast('' as STRING) as USER_COMT_TXT,\ncast('' as STRING) as USER_PRFL_ID,\ncast('' as STRING) as WEB_DRILLDOWN_MENU_DEF_ID,\ncast('' as STRING) as WEB_MENU_DEF_ID,\ncast('' as STRING) as ESIG_ROLE_GRP_ID,\ncast('' as STRING) as WW_ID,\ncast('' as STRING) as ES_ROLE_GRP_ID,\ncast('' as STRING) as FIL_TAG_ACCS_CD,\ncast('' as STRING) as FIL_TAGS_SESS_CD,\ncast('' as STRING) as FIL_TAGS_TEXT,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
