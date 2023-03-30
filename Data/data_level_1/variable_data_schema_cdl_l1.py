import datetime
from decimal import *
import os

tables = {
    "MD_HU_SER","MD_MATL_INV_SLS"
}
env =os.getenv("DATABRICKS_ENV").lower()
loc_MD_HU_SER= "dbfs:/mnt/" + os.getenv("DATABRICKS_ENV").lower() + "_curdelta/md_l1/MD_HANDLING_UNIT/MD_HU_SER"
loc_MD_MATL_INV_SLS = "dbfs:/mnt/" + os.getenv("DATABRICKS_ENV").lower() + "_curdelta/md_l1/MD_INVENTORY_STOCK/MD_MATL_INV_SLS"
table_number_of_columns = {"MD_HU_SER":"13","MD_MATL_INV_SLS":"20"}
table_edm_location = {"MD_HU_SER":loc_MD_HU_SER,"MD_MATL_INV_SLS":loc_MD_MATL_INV_SLS}
env = os.getenv("DATABRICKS_ENV").lower()
table = "MD_HU_SER"
column_data_Types ={"MD_HU_SER" : {'SRC_SYS_CD' : 'string','HU_SER_NUM' : 'int','HU_NUM' : 'string','EXTRNL_HU_NUM' : 'string','HU_LINE_NUM' : 'string','DAI_ETL_ID' : 'int','DAI_CRT_DTTM' : 'timestamp','DAI_UPDT_DTTM' : 'timestamp','_l0_upt_' : 'timestamp','_l1_upt_' : 'timestamp','_pk_md5_' : 'string','_pk_' : 'string','_deleted_':'string'},"MD_MATL_INV_SLS": {'SRC_SYS_CD' : 'string','MATL_NUM' : 'string','PLNT_CD' : 'string','SLOC_CD' : 'string','BTCH_NUM' : 'string','SPCL_STCK_IND' : 'string','SLS_ORDR_NUM' : 'string','SLS_ORDR_LINE_NBR' : 'string','UNRSTRCTD_STCK' : 'decimal(18,4)','QLTY_INSP_STCK' : 'decimal(18,4)','BLCKD_STCK' : 'decimal(18,4)','RSTRCTD_STCK' : 'decimal(18,4)','DAI_ETL_ID' : 'int','DAI_CRT_DTTM' : 'timestamp','DAI_UPDT_DTTM' : 'timestamp','_l0_upt_' : 'timestamp','_l1_upt_' : 'timestamp','_pk_md5_' : 'string','_pk_' : 'string','_deleted_':'string'}}