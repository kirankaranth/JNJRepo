from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.graph.NEW_FIELDS_RENAME_FORMAT import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("DEC_PLACE_CNT", "CRNCY_LONG_NM"),
            dfOutComputed.select("DEC_PLACE_CNT", "CRNCY_LONG_NM"),
            self.maxUnequalRowsToShow
        )

    def test_int(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_int.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_int.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("NUM_OF_DEC_PLACES"),
            dfOutComputed.select("NUM_OF_DEC_PLACES"),
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
