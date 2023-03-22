from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.A6AN8") == col("in1.ABAN8")), "left_outer")\
        .select(col("in0.A6AN8").alias("A6AN8"), col("in0._deleted_").alias("_deleted_"), col("in0.A6APC").alias("A6APC"), col("in0.A6MCUP").alias("A6MCUP"), col("in0.A6OBAP").alias("A6OBAP"), col("in0.A6AIDP").alias("A6AIDP"), col("in0.A6KCOP").alias("A6KCOP"), col("in0.A6DCAP").alias("A6DCAP"), col("in0.A6DTAP").alias("A6DTAP"), col("in0.A6CRRP").alias("A6CRRP"), col("in0.A6TXA2").alias("A6TXA2"), col("in0.A6EXR2").alias("A6EXR2"), col("in0.A6HDPY").alias("A6HDPY"), col("in0.A6TXA3").alias("A6TXA3"), col("in0.A6EXR3").alias("A6EXR3"), col("in0.A6TAWH").alias("A6TAWH"), col("in0.A6PCWH").alias("A6PCWH"), col("in0.A6TRAP").alias("A6TRAP"), col("in0.A6SCK").alias("A6SCK"), col("in0.A6PYIN").alias("A6PYIN"), col("in0.A6SNTO").alias("A6SNTO"), col("in0.A6AB1").alias("A6AB1"), col("in0.A6FLD").alias("A6FLD"), col("in0.A6SQNL").alias("A6SQNL"), col("in0.A6CRCA").alias("A6CRCA"), col("in0.A6AYPD").alias("A6AYPD"), col("in0.A6APPD").alias("A6APPD"), col("in0.A6ABAM").alias("A6ABAM"), col("in0.A6ABA1").alias("A6ABA1"), col("in0.A6APRC").alias("A6APRC"), col("in0.A6MINO").alias("A6MINO"), col("in0.A6MAXO").alias("A6MAXO"), col("in0.A6AN8R").alias("A6AN8R"), col("in0.A6BADT").alias("A6BADT"), col("in0.A6CPGP").alias("A6CPGP"), col("in0.A6ORTP").alias("A6ORTP"), col("in0.A6INMG").alias("A6INMG"), col("in0.A6HOLD").alias("A6HOLD"), col("in0.A6ROUT").alias("A6ROUT"), col("in0.A6STOP").alias("A6STOP"), col("in0.A6ZON").alias("A6ZON"), col("in0.A6ANCR").alias("A6ANCR"), col("in0.A6CARS").alias("A6CARS"), col("in0.A6DEL1").alias("A6DEL1"), col("in0.A6DEL2").alias("A6DEL2"), col("in0.A6LTDT").alias("A6LTDT"), col("in0.A6FRTH").alias("A6FRTH"), col("in0.A6INVC").alias("A6INVC"), col("in0.A6PLST").alias("A6PLST"), col("in0.A6WUMD").alias("A6WUMD"), col("in0.A6VUMD").alias("A6VUMD"), col("in0.A6PRP5").alias("A6PRP5"), col("in0.A6EDPM").alias("A6EDPM"), col("in0.A6EDCI").alias("A6EDCI"), col("in0.A6EDII").alias("A6EDII"), col("in0.A6EDQD").alias("A6EDQD"), col("in0.A6EDAD").alias("A6EDAD"), col("in0.A6EDF1").alias("A6EDF1"), col("in0.A6EDF2").alias("A6EDF2"), col("in0.A6VI01").alias("A6VI01"), col("in0.A6VI02").alias("A6VI02"), col("in0.A6VI03").alias("A6VI03"), col("in0.A6VI04").alias("A6VI04"), col("in0.A6VI05").alias("A6VI05"), col("in0.A6MNSC").alias("A6MNSC"), col("in0.A6ATO").alias("A6ATO"), col("in0.A6RVNT").alias("A6RVNT"), col("in0.A6URCD").alias("A6URCD"), col("in0.A6URDT").alias("A6URDT"), col("in0.A6URAT").alias("A6URAT"), col("in0.A6URAB").alias("A6URAB"), col("in0.A6URRF").alias("A6URRF"), col("in0.A6USER").alias("A6USER"), col("in0.A6PID").alias("A6PID"), col("in0.A6JOBN").alias("A6JOBN"), col("in0.A6UPMJ").alias("A6UPMJ"), col("in0.A6UPMT").alias("A6UPMT"), col("in0.A6ASN").alias("A6ASN"), col("in0.A6CRMD").alias("A6CRMD"), col("in0.A6AVCH").alias("A6AVCH"), col("in0._pk_").alias("_pk_"), col("in0._upt_").alias("_upt_"), col("in0._kafkaOffset_").alias("_kafkaOffset_"), col("in0._createTime_").alias("_createTime_"), col("in0._kafkaTimestamp_").alias("_kafkaTimestamp_"), col("in0._infa_bigint_sequence_").alias("_infa_bigint_sequence_"), col("in1.ABMCU").alias("F0101_ABMCU"))
