from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def F4101_SELECTION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("IMAITM"), 
        col("IMDRAW"), 
        col("IMDSC1"), 
        col("IMDSC2"), 
        col("IMGLPT"), 
        col("IMITM"), 
        col("IMLITM"), 
        col("IMLNTY"), 
        col("IMPRP0"), 
        col("IMPRP1"), 
        col("IMPRP2"), 
        col("IMPRP3"), 
        col("IMPRP4"), 
        col("IMPRP5"), 
        col("IMPRP6"), 
        col("IMPRP7"), 
        col("IMPRP8"), 
        col("IMPRP9"), 
        col("IMRVNO"), 
        col("IMSLD"), 
        col("IMSRCE"), 
        col("IMSRP0"), 
        col("IMSRP1"), 
        col("IMSRP2"), 
        col("IMSRP3"), 
        col("IMSRP4"), 
        col("IMSRP5"), 
        col("IMSRP6"), 
        col("IMSRP7"), 
        col("IMSRP8"), 
        col("IMSRP9"), 
        col("IMSTKT"), 
        col("IMUOM1"), 
        col("IMUPMJ"), 
        col("IMTDAY"), 
        col("IMUSER"), 
        col("IMUVM1"), 
        col("IMUWUM"), 
        col("IMVCUD"), 
        col("_deleted_"), 
        expr(Config.B_M_LU_Field).alias("B_M_LU"), 
        expr(Config.F_C_LU_Field).alias("F_C_LU"), 
        expr(Config.M_G_LU_Field).alias("M_G_LU"), 
        expr(Config.M_T_D_LU_Field).alias("M_T_D_LU"), 
        col("IMMPST"), 
        col("IMPTSC"), 
        col("_upt_"), 
        expr(Config.M_G_2_Field).alias("M_G2_LU")
    )
