from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd.graph.SchemaTransform_MCHA import *
from sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd.config.ConfigStore import *


class SchemaTransform_MCHATest(BaseTestCase):

    def test_pk(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/in0/schema.json',
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/in0/data/test_pk.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/out/schema.json',
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/out/data/test_pk.json',
            'out'
        )
        dfOutComputed = SchemaTransform_MCHA(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("SRC_SYS_CD", "SRC_TBL_NM", "MATL_NUM", "BTCH_NUM"),
            dfOutComputed.select("SRC_SYS_CD", "SRC_TBL_NM", "MATL_NUM", "BTCH_NUM"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/in0/schema.json',
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/in0/data/test_timestamp.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/out/schema.json',
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/out/data/test_timestamp.json',
            'out'
        )
        dfOutComputed = SchemaTransform_MCHA(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CRT_DTTM"), dfOutComputed.select("CRT_DTTM"), self.maxUnequalRowsToShow)

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/in0/schema.json',
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/out/schema.json',
            'test/resources/data/sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd/graph/SchemaTransform_MCHA/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = SchemaTransform_MCHA(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("DEL_IND", "SUP_NUM", "SUP_BTCH_NUM"),
            dfOutComputed.select("DEL_IND", "SUP_NUM", "SUP_BTCH_NUM"),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
