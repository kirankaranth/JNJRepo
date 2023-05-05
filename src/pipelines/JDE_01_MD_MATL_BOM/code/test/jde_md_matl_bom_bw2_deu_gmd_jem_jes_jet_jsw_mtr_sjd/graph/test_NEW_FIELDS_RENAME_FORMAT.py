from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd.graph.NEW_FIELDS_RENAME_FORMAT import *
from jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/jde_md_matl_bom_bw2_deu_gmd_jem_jes_jet_jsw_mtr_sjd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("MATL_NUM", "PLNT_CD", "BOM_USG_CD", "BOM_NUM"),
            dfOutComputed.select("MATL_NUM", "PLNT_CD", "BOM_USG_CD", "BOM_NUM"),
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
