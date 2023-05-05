from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.graph.NEW_FIELDS_01 import *
from jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr.config.ConfigStore import *


class NEW_FIELDS_01Test(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr/graph/NEW_FIELDS_01/in0/schema.json',
            'test/resources/data/jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr/graph/NEW_FIELDS_01/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr/graph/NEW_FIELDS_01/out/schema.json',
            'test/resources/data/jde_md_matl_uom_jet_jes_jem_jsw_bw2_djd_sjd_deu_gmd_mtr/graph/NEW_FIELDS_01/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_01(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("UOM_CD", "DIM_KEY", "UOM_SHRT_TEXT", "UOM_LONG_TEXT"),
            dfOutComputed.select("UOM_CD", "DIM_KEY", "UOM_SHRT_TEXT", "UOM_LONG_TEXT"),
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
