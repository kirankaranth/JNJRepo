from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MD_MATL_2.config.ConfigStore import *
from PPLN_MD_MATL_2.udfs.UDFs import *

def sql_MD_MATL(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    WITH cte_atwrt AS
  (SELECT objek,
          atwrt_material_spec,
          atwrt_spec_rev_level
   FROM
     (SELECT inob.objek,
             ausp.atwrt,
             cabn.atnam
      FROM {Config.sourceDatabase}.inob
      INNER JOIN {Config.sourceDatabase}.ausp ON ausp.objek = inob.cuobj
      INNER JOIN {Config.sourceDatabase}.cabn ON cabn.atinn = ausp.atinn
      WHERE inob.obtab = 'MARA'
        AND inob.klart = '001'
        AND UPPER(cabn.atnam) IN ('MATERIAL_SPEC',
                                  'SPEC_REV_LEVEL')
  AND inob._deleted_ = 'F' AND inob.MANDT = '100'
        AND ausp._deleted_ = 'F' AND ausp.MANDT = '100'
        AND cabn._deleted_ = 'F' AND cabn.MANDT = '100' ) PIVOT(MAX(atwrt) FOR atnam IN ('MATERIAL_SPEC' AS atwrt_material_spec, 'SPEC_REV_LEVEL' AS atwrt_spec_rev_level)))     
SELECT
    '{Config.sourceSystem}'   AS SRC_SYS_CD
,MARA.matnr AS MATL_NUM
,TRIM(MARA.mtart) AS MATL_TYPE_CD
,NULL AS BRND_CD
,TRIM(MARA.spart) AS FRANCHISE_CD
,TRIM(MARA.labor) AS LCL_PLNG_SUB_FRAN_CD
,TRIM(MARA.ekwsl) AS PRCHSNG_VAL_KEY_CD
,TRIM(MARA.lvorm) AS DEL_IND
,TRIM(MARA.matkl) AS MATL_GRP_CD
,TRIM(MARA.mbrsh) AS INDSTR_SECTR_CD
,TRIM(MARA.meins) AS BASE_UOM_CD
,CAST(MARA.mhdhb AS DECIMAL(18,4)) AS TOT_SHLF_LIF_DAYS_CNT
,CAST(MARA.mhdrz AS DECIMAL(18,4)) AS MIN_SHLF_RMN_LIF_DAYS_CNT
,TRIM(MARA.mstae) AS MATL_STS_CD
,TRIM(MARA.mstav) AS DSTN_CHN_STS_CD
,CAST(MARA.ntgew AS DECIMAL(18,4)) AS NET_WT_MEAS
,TRIM(MARA.prdha) AS PROD_HIER_CD
,TRIM(MARA.qmpur) AS PRCMT_QUAL_MGMT_IND
,TRIM(MARA.raube) AS STRG_CONDS_CD
,TRIM(MARA.tempb) AS LBL_TEMP_RNG
,TRIM(MARA.tragr) AS TRSPN_GRP_CD
,TRIM(MARA.xchpf) AS BTCH_MNG_IND
,TRIM(MARA.zeinr) AS MATL_DOC_NUM
,TRIM(MARA.zeivr) AS MATL_DOC_VERS_NUM
,TRIM(MAKT.maktx) AS MATL_SHRT_DESC
,TRIM(MARA.zzmmssurgtype) AS MMS_SURGERY_TYPE_CD
,TRIM(MARA.zzmmstype) AS MMS_MATL_TYPE_CD
,TRIM(MARA.zzwerks) AS PRMRY_PLNT_CD
,TRIM(MARA.zzmmsficlass) AS MMS_FIN_CLSN_CD
,TRIM(MARA.zzmmsterile) AS MMS_STERILIZATION_IND
,TRIM(MARA.zzcatnumber) AS MATL_CATLG_NUM
,TRIM(MARA.zzsector) AS SRC_SECTR_CD
,TRIM(MARA.zzp2_basecode) AS MATL_PARNT_CD
,TRIM(MARA.zzmatsub_type) AS MATL_SUB_TYPE_CD
,NULL AS FIN_HIER_BASE_CD
,TRIM(MARA.zzprod_cat1) AS IMPLNT_INSTM_IND
,TRIM(MARA.zzvariant) AS MATL_MOD_CD
,TRIM(MARA.zzkit_ind) AS KIT_IND
,TRIM(MARA.zzmmsts) AS MMS_TEMP_SENS_IND
,NULL AS DIR_PART_MRKNG_CD
,TRIM(MARA.mtpos_mara) AS MATL_CAT_GRP_CD
,TRIM(MARA.zzp3_low_level) AS PLNG_HIER3_CD
,TRIM(cte_atwrt.atwrt_material_spec) AS MATL_SPEC_NUM
,TRIM(cte_atwrt.atwrt_spec_rev_level) AS MATL_SPEC_VERS_NUM
,NULL AS CHG_BY
,NULL AS DOC_CHG_NUM
,NULL AS CNTNR_REQ
,NULL AS OLD_MATL_NUM
,NULL AS GRS_WT
,NULL AS ORDR_UNIT_PUR_UOM
,NULL AS CHEM_CMPLI
,NULL AS CRT_BY
,NULL AS CRT_ON_DTTM
,NULL AS LBL_TYPE
,NULL AS LBL_FORM
,NULL AS EXTRNL_MATL_GRP
,NULL AS MAX_LVL
,NULL AS WT_UNIT
,NULL AS SIZE_DIM
,NULL AS PER_IN
,NULL AS LAST_CHG_DT_TIME_DTTM
,NULL AS MATL_GRP_PKGNG_MATL
,NULL AS MATL_EXTRNL
,NULL AS STRG_PCT
,NULL AS VAL_FROM_XPLNT_DTTM
,NULL AS VAL_FROM_XDC_DTTM
,NULL AS INDSTR_STD_DESC
,NULL AS RD_RUL_SLED
,NULL AS SER_LVL
,NULL AS MATL_HAZ_CD
,NULL AS VAR_ORDR_UNT
,NULL AS PKGNG_MATL_TYPE
,NULL AS VOL_UNIT
,NULL AS VOL
,NULL AS BSC_MATL
,NULL AS DOC_TYPE
,NULL AS DOC_PG_FMT
,NULL AS EAN_UPC
,NULL AS EAN_CAT
,NULL AS MFR_BOOK_PART_NUM
,NULL AS DIR_SHP_FL
,NULL AS FIN_PLNT
,NULL AS MAIN_STRG_LOC
,NULL AS PCS_PER_SLS_UNT
,NULL AS PROD_LINE
,NULL AS MAKE_BUY_IN
,NULL AS STRT_PLNT
,NULL AS MATL_SHRT_DESC_UP_CASE
,NULL AS MATL_TYPE_DESC
,NULL AS FRAN_CD_DESC
,NULL AS BASE_UOM_DESC
,NULL AS OBJ_NUM
,NULL AS TYPE_OF_MATERIAL
,NULL AS STERILE
,NULL AS BRAVO_MINOR_CODE
,NULL AS BRAVO_MINOR_CODE_DESC
,NULL AS NDL_SLS_TYPE
,NULL AS SUTURE_LENGTH_INCH
,NULL AS SER_TYPE
,NULL AS GTIN_VRNT
,NULL AS MATL_GRP_DESC
,NULL AS MATL_GRP_DESC_2
,'#' AS SHRT_MATL_NUM
,NULL AS CMMDTY
,NULL AS NDL_COLOR
,NULL AS NDL_ALLOY
,NULL AS SUTURE_USP
,NULL AS EAN_UPC_HRMZD
FROM
  {Config.sourceDatabase}.MARA MARA
  LEFT JOIN {Config.sourceDatabase}.MAKT MAKT on MARA.matnr = MAKT.matnr
  and MAKT.spras = 'E' and MAKT._deleted_ = 'F' and MAKT.MANDT = '100'
  left join (
        select
          INOB.OBJEK,
    row_number() OVER (PARTITION BY OBJEK order by OBJEK DESC) AS row_num
  from
          {Config.sourceDatabase}.INOB INOB
        where
          INOB.KLART = 001
          AND INOB.OBTAB='MARA'
          AND INOB._deleted_ = 'F'
          AND INOB.MANDT =\"100\"
      ) INOB on MARA.MATNR = INOB.OBJEK 
  and INOB.row_num = 1
  LEFT JOIN cte_atwrt ON cte_atwrt.objek = MARA.matnr
WHERE
   MARA._deleted_ = 'F' and MARA.MANDT ='100'  
 
"""
    )

    return out0
