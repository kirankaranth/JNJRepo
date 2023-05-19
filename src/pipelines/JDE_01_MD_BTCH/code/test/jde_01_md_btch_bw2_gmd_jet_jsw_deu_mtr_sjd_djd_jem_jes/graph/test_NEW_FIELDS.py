from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.graph.NEW_FIELDS import *
from jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_pk(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS/in0/data/test_pk.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS/out/data/test_pk.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("SRC_TBL_NM", "MATL_NUM", "BTCH_NUM", "PLNT_CD", "SHRT_MATL_NUM"),
            dfOutComputed.select("SRC_TBL_NM", "MATL_NUM", "BTCH_NUM", "PLNT_CD", "SHRT_MATL_NUM"),
            self.maxUnequalRowsToShow
        )

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("BTCH_STS_CD", "SUP_NUM", "SUP_BTCH_NUM", "BTCH_TYPE"),
            dfOutComputed.select("BTCH_STS_CD", "SUP_NUM", "SUP_BTCH_NUM", "BTCH_TYPE"),
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
