from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn.config.ConfigStore import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn.udfs.UDFs import *

def FIELDS_VBFA(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("VBTYP_N"), 
        col("VBELV"), 
        col("VBELN"), 
        col("POSNV"), 
        col("POSNN"), 
        col("RFMNG"), 
        col("MEINS"), 
        col("RFWRT"), 
        col("WAERS"), 
        col("VBTYP_V"), 
        col("PLMIN"), 
        col("TAQUI"), 
        col("ERDAT"), 
        col("ERZET"), 
        col("MATNR"), 
        col("BWART"), 
        col("BDART"), 
        col("PLART"), 
        col("STUFE"), 
        col("LGNUM"), 
        col("AEDAT"), 
        col("FKTYP"), 
        col("BRGEW"), 
        col("GEWEI"), 
        col("VOLUM"), 
        col("VOLEH"), 
        col("FPLNR"), 
        col("FPLTR"), 
        col("RFMNG_FLO"), 
        col("RFMNG_FLT"), 
        col("VRKME"), 
        col("ABGES"), 
        col("SOBKZ"), 
        col("SONUM"), 
        col("KZBEF"), 
        col("NTGEW"), 
        col("LOGSYS"), 
        col("WBSTA"), 
        col("CMETH"), 
        col("MJAHR"), 
        col("VBTYPEXT_V"), 
        col("VBTYPEXT_N")
    )
