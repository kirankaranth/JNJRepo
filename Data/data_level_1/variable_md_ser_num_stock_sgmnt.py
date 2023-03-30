import datetime
from decimal import *
import os

table = "MD_SER_NUM_STOCK_SGMNT"
env = os.getenv("DATABRICKS_ENV").lower()
view = "v01_md_ser_num_stock_sgmnt"
underlying_table = "md_equipment_MD_SER_NUM_STOCK_SGMNT"
source_system = {"atl", "bba", "bbl", "bbn", "hcs", "mbp", "mrs", "p01", "svs"}
LIST__COLUMNS_WITH_PREDEFINED_VALUES_PER_SOURCE_EXTEND = {
    "atl [SRC_SYS_CD:atl]",
    "bba [SRC_SYS_CD:bba]",
    "bbl [SRC_SYS_CD:bbl]",
    "bbn [SRC_SYS_CD:bbn]",
    "hcs [SRC_SYS_CD:hcs]",
    "mbp [SRC_SYS_CD:mbp]",
    "mrs [SRC_SYS_CD:mrs]",
    "p01 [SRC_SYS_CD:p01]",
    "svs [SRC_SYS_CD:svs]"
}
LIST__PRIMARY_KEYS = {"SRC_SYS_CD, EQMNT_NUM"}
COLUMN_COUNT = 16
LIST__UTC_COLUMNS = ["DAI_CRT_DTTM", "DAI_UPDT_DTTM"]
WHITESPACE_SOURCES_1 = {"atl","bba","bbl","bbn","hcs"}
WHITESPACE_COLUMNS_1 = [
	"STOCK_TYPE_GOODS_MVMT",
	"PLNT",
	"STRG_LOC",
	"BTCH_NUM",
	"SPL_STOCK_IN",
	"SPL_STOCK_CUST_ACCT_NUM",
	"ACCT_NUM_VEND",
	"SLS_ORDR_NUM",
	"ITM_NUM_SLS_ORDR",
	"WBS_ELMNT",
	"OWN_STOCK"
]
WHITESPACE_SOURCES_2 = {"mbp","mrs","p01","svs"}
WHITESPACE_COLUMNS_2 = [
	"STOCK_TYPE_GOODS_MVMT",
	"PLNT",
	"STRG_LOC",
	"BTCH_NUM",
	"SPL_STOCK_IN",
	"SPL_STOCK_CUST_ACCT_NUM",
	"ACCT_NUM_VEND",
	"SLS_ORDR_NUM",
	"ITM_NUM_SLS_ORDR",
	"WBS_ELMNT"
	]
LIST__NULL_SOURCES = {"hcs", "mbp","p01","svs"}
LIST__NULL_COLUMNS = ["OWN_STOCK"]
