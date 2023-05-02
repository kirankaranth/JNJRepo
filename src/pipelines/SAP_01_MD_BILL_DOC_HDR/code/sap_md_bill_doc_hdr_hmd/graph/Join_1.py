from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bill_doc_hdr_hmd.config.ConfigStore import *
from sap_md_bill_doc_hdr_hmd.udfs.UDFs import *

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
        .join(in1.alias("in1"), (col("in0.SPART") == col("in1.SPART")), "left_outer")\
        .join(in2.alias("in2"), (col("in0.FKART") == col("in2.FKART")), "left_outer")\
        .join(in3.alias("in3"), (col("in0.VKORG") == col("in3.VKORG")), "left_outer")\
        .join(in4.alias("in4"), (col("in0.VTWEG") == col("in4.VTWEG")), "left_outer")\
        .select(col("in0.VBELN").alias("VBELN"), col("in0.VKORG").alias("VKORG"), col("in3.VTEXT").alias("TVKOT_VTEXT"), col("in0.VTWEG").alias("VTWEG"), col("in4.VTEXT").alias("TVTWT_VTEXT"), col("in0.SPART").alias("SPART"), col("in1.VTEXT").alias("TSPAT_VTEXT"), col("in0.FKART").alias("FKART"), col("in2.VTEXT").alias("TVFKT_VTEXT"), col("in0.FKTYP").alias("FKTYP"), col("in0.VBTYP").alias("VBTYP"), col("in0.KUNRG").alias("KUNRG"), col("in0.KUNAG").alias("KUNAG"), col("in0.KUNWE").alias("KUNWE"), col("in0.ERDAT").alias("ERDAT"), col("in0.ERZET").alias("ERZET"), col("in0.FKDAT").alias("FKDAT"), col("in0.FKDAT_RL").alias("FKDAT_RL"), col("in0.WAERK").alias("WAERK"), col("in0.ERNAM").alias("ERNAM"), col("in0.KALSM").alias("KALSM"), col("in0.KNUMV").alias("KNUMV"), col("in0.VSBED").alias("VSBED"), col("in0.GJAHR").alias("GJAHR"), col("in0.POPER").alias("POPER"), col("in0.KDGRP").alias("KDGRP"), col("in0.INCO1").alias("INCO1"), col("in0.INCO2").alias("INCO2"), col("in0.RFBSK").alias("RFBSK"), col("in0.KURRF").alias("KURRF"), col("in0.VALTG").alias("VALTG"), col("in0.VALDT").alias("VALDT"), col("in0.ZTERM").alias("ZTERM"), col("in0.KTGRD").alias("KTGRD"), col("in0.LAND1").alias("LAND1"), col("in0.REGIO").alias("REGIO"), col("in0.BUKRS").alias("BUKRS"), col("in0.TAXK1").alias("TAXK1"), col("in0.NETWR").alias("NETWR"), col("in0.ZUKRI").alias("ZUKRI"), col("in0.STWAE").alias("STWAE"), col("in0.AEDAT").alias("AEDAT"), col("in0.FKART_RL").alias("FKART_RL"), col("in0.KKBER").alias("KKBER"), col("in0.KNKLI").alias("KNKLI"), col("in0.CMWAE").alias("CMWAE"), col("in0.CMKUF").alias("CMKUF"), col("in0.HITYP_PR").alias("HITYP_PR"), col("in0.BSTNK_VF").alias("BSTNK_VF"), col("in0.VBUND").alias("VBUND"), col("in0.LANDTX").alias("LANDTX"), col("in0.STCEG_H").alias("STCEG_H"), col("in0.STCEG_L").alias("STCEG_L"), col("in0.XBLNR").alias("XBLNR"), col("in0.ZUONR").alias("ZUONR"), col("in0.MWSBK").alias("MWSBK"), col("in0.LOGSYS").alias("LOGSYS"), col("in0.KURRF_DAT").alias("KURRF_DAT"), col("in0.KIDNO").alias("KIDNO"), col("in0.NUMPG").alias("NUMPG"), col("in0.BUCHK").alias("BUCHK"), col("in0.RELIK").alias("RELIK"), col("in0.KONDA").alias("KONDA"), col("in0.BZIRK").alias("BZIRK"), col("in0.PLTYP").alias("PLTYP"), col("in0.TAXK2").alias("TAXK2"), col("in0.TAXK3").alias("TAXK3"), col("in0.TAXK4").alias("TAXK4"), col("in0.TAXK5").alias("TAXK5"), col("in0.TAXK6").alias("TAXK6"), col("in0.TAXK7").alias("TAXK7"), col("in0.TAXK8").alias("TAXK8"), col("in0.TAXK9").alias("TAXK9"), col("in0.FKSTO").alias("FKSTO"), col("in0._upt_").alias("_upt_"))
