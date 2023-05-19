from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_matl_loc_jet_jsw_mtr_bw2_gmd_deu_jem_jes_sjd_djd.graph.NEW_FIELDS_PK import *
from jde_md_matl_loc_jet_jsw_mtr_bw2_gmd_deu_jem_jes_sjd_djd.config.ConfigStore import *


class NEW_FIELDS_PKTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_loc_jet_jsw_mtr_bw2_gmd_deu_jem_jes_sjd_djd/graph/NEW_FIELDS_PK/in0/schema.json',
            'test/resources/data/jde_md_matl_loc_jet_jsw_mtr_bw2_gmd_deu_jem_jes_sjd_djd/graph/NEW_FIELDS_PK/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_loc_jet_jsw_mtr_bw2_gmd_deu_jem_jes_sjd_djd/graph/NEW_FIELDS_PK/out/schema.json',
            'test/resources/data/jde_md_matl_loc_jet_jsw_mtr_bw2_gmd_deu_jem_jes_sjd_djd/graph/NEW_FIELDS_PK/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_PK(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("LOT_SIZE_VAL", "RD_VAL_QTY", "LOT_SIZE_MAX_QTY"),
            dfOutComputed.select("LOT_SIZE_VAL", "RD_VAL_QTY", "LOT_SIZE_MAX_QTY"),
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
