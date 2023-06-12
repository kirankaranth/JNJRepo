from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd.graph.NEW_FIELDS_RENAME_FORMAT import *
from jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_trim_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_trim_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_trim_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("PLNT_CD"), dfOutComputed.select("PLNT_CD"), self.maxUnequalRowsToShow)

    def test_decimal_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_decimal_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_decimal_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("ITM_QTY"), dfOutComputed.select("ITM_QTY"), self.maxUnequalRowsToShow)

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("_upt_"), dfOutComputed.select("_upt_"), self.maxUnequalRowsToShow)

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
