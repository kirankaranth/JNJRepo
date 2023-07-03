from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from PPLN_MES_MD_RECIPE_SPEC_ITM_1.config.ConfigStore import *
from PPLN_MES_MD_RECIPE_SPEC_ITM_1.udfs.UDFs import *

def sql_MES_MD_RECIPE_SPEC_ITM(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
     '{Config.sourceSystem}' AS SRC_SYS_CD,
trim(CcfRecipeSpecItem.ccfrecipespecitemid) AS RECIPE_SPEC_ITM_ID,
trim(CcfRecipeSpecItem.ccfproductrecipeid) AS PROD_RVSN_ID,
cast(CcfRecipeSpecItem.ChangeCount as INT) AS CHG_CNT,
trim(CcfRecipeSpecItem.ccfdatapointname) AS DATA_PT_NM,
trim(CcfRecipeSpecItem.ccfdatatype) AS DATA_TYPE_CD,
NULL AS EXPT_IMPT_KEY_VAL,
cast(CcfRecipeSpecItem.isfrozen AS BOOLEAN) AS IS_FRZN_IND,
trim(CcfRecipeSpecItem.ccfitemname) AS ITM_NM,
trim(CcfRecipeSpecItem.ccfitemrevision) AS ITM_RVSN_ID,
trim(CcfRecipeSpecItem.ccflowerlimit) AS LWR_LMT_VAL,
trim(CcfRecipeSpecItem.ccflowerwarninglimit) AS LWR_WARN_LMT_VAL,
trim(CcfRecipeSpecItem.ccfnominalvalue) AS NOM_VAL,
trim(CcfRecipeSpecItem.cdotypeid) AS OBJ_TYPE_ID,
trim(CcfRecipeSpecItem.ccfrecipecategoryid) AS RECIPE_CAT_ID,
NULL AS RESL_LWR_LMT_VAL,
NULL AS RESL_LWR_WARN_VAL,
NULL AS RESL_UP_LMT_VAL,
NULL AS RESL_UP_WARN_VAL,
trim(CcfRecipeSpecItem.ccfupperlimit) AS UP_LMT_VAL,
trim(CcfRecipeSpecItem.ccfupperwarninglimit) AS UP_WARN_LMT_VAL
FROM {Config.sourceDatabase}.CcfRecipeSpecItem CcfRecipeSpecItem
WHERE CcfRecipeSpecItem._deleted_='F'  
 
"""
    )

    return out0
