from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def Join_1(
        spark: SparkSession,
        in0: DataFrame,
        in1: DataFrame,
        in2: DataFrame, 
        in3: DataFrame, 
        in4: DataFrame
) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.MMSTA") == col("in1.MMSTA")), "left_outer")\
        .join(in2.alias("in2"), (col("in0.DISPO") == col("in2.DISPO")), "left_outer")\
        .join(
          in3.alias("in3"),
          ((col("in0.WERKS") == col("in3.WERKS")) & (col("in0.FEVOR") == col("in3.FEVOR"))),
          "left_outer"
        )\
        .join(in4.alias("in4"), (col("in0.EKGRP") == col("in4.EKGRP")), "left_outer")\
        .select(col("in0.WERKS").alias("NSDM_V_MARC_WERKS"), col("in0.MATNR").alias("NSDM_V_MARC_MATNR"), col("in0.AWSLS").alias("AWSLS"), col("in0.KAUTB").alias("KAUTB"), col("in0.PRCTR").alias("PRCTR"), col("in0.KORDB").alias("KORDB"), col("in0.LADGR").alias("LADGR"), col("in0.SSQSS").alias("SSQSS"), col("in0.MINBE").alias("MINBE"), col("in0.LOSGR").alias("LOSGR"), col("in0.SERNP").alias("SERNP"), col("in0.MMSTA").alias("MMSTA"), col("in0.MMSTD").alias("MMSTD"), col("in0.LVORM").alias("LVORM"), col("in0.DISLS").alias("DISLS"), col("in0.DISPO").alias("DISPO"), col("in0.FEVOR").alias("FEVOR"), col("in0.FRTME").alias("FRTME"), col("in0.MAABC").alias("MAABC"), col("in0.MTVFP").alias("MTVFP"), col("in0.SHFLG").alias("SHFLG"), col("in0.SOBSL").alias("SOBSL"), col("in0.BESKZ").alias("BESKZ"), col("in0.DISMM").alias("DISMM"), col("in0.HERKL").alias("HERKL"), col("in0.STRGR").alias("STRGR"), col("in0.BSTRF").alias("BSTRF"), col("in0.BSTFE").alias("BSTFE"), col("in0.WEBAZ").alias("WEBAZ"), col("in0.BSTMA").alias("BSTMA"), col("in0.BSTMI").alias("BSTMI"), col("in0.EISBE").alias("EISBE"), col("in0.FXHOR").alias("FXHOR"), col("in0.MABST").alias("MABST"), col("in0.SHZET").alias("SHZET"), col("in0.PLIFZ").alias("PLIFZ"), col("in0.AUSSS").alias("AUSSS"), col("in0.DZEIT").alias("DZEIT"), col("in0.EISLO").alias("EISLO"), col("in0.VINT1").alias("VINT1"), col("in0.VINT2").alias("VINT2"), col("in0.VRMOD").alias("VRMOD"), col("in0.EKGRP").alias("EKGRP"), col("in0.BWTTY").alias("BWTTY"), col("in0.XCHAR").alias("XCHAR"), col("in0.DISPR").alias("DISPR"), col("in0.PERKZ").alias("PERKZ"), col("in0.SBDKZ").alias("SBDKZ"), col("in0.LAGPR").alias("LAGPR"), col("in0.KZAUS").alias("KZAUS"), col("in0.AUSDT").alias("AUSDT"), col("in0.NFMAT").alias("NFMAT"), col("in0.KZBED").alias("KZBED"), col("in0.MISKZ").alias("MISKZ"), col("in0.BASMG").alias("BASMG"), col("in0.MAXLZ").alias("MAXLZ"), col("in0.LZEIH").alias("LZEIH"), col("in0.UEETO").alias("UEETO"), col("in0.UEETK").alias("UEETK"), col("in0.UNETO").alias("UNETO"), col("in0.WZEIT").alias("WZEIT"), col("in0.INSMK").alias("INSMK"), col("in0.XCHPF").alias("XCHPF"), col("in0.PERIV").alias("PERIV"), col("in0.STAWN").alias("STAWN"), col("in0.HERKR").alias("HERKR"), col("in0.EXPME").alias("EXPME"), col("in0.SAUFT").alias("SAUFT"), col("in0.VERKZ").alias("VERKZ"), col("in0.STLAL").alias("STLAL"), col("in0.STLAN").alias("STLAN"), col("in0.PLNNR").alias("PLNNR"), col("in0.APLAL").alias("APLAL"), col("in0.LGPRO").alias("LGPRO"), col("in0.DISGR").alias("DISGR"), col("in0.KAUSF").alias("KAUSF"), col("in0.QZGTP").alias("QZGTP"), col("in0.QMATV").alias("QMATV"), col("in0.ABCIN").alias("ABCIN"), col("in0.STDPD").alias("STDPD"), col("in0.SFEPR").alias("SFEPR"), col("in0.XMCNG").alias("XMCNG"))
