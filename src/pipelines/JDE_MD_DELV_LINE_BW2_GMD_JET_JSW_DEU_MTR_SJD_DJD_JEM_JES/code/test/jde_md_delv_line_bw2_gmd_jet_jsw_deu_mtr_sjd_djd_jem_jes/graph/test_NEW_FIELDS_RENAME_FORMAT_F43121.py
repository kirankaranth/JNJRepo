from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.graph.NEW_FIELDS_RENAME_FORMAT_F43121 import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMAT_F43121Test(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS_RENAME_FORMAT_F43121/in0/schema.json',
            'test/resources/data/jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS_RENAME_FORMAT_F43121/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS_RENAME_FORMAT_F43121/out/schema.json',
            'test/resources/data/jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS_RENAME_FORMAT_F43121/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT_F43121(self.spark, dfIn0)
        assertDFEquals(dfOut.select("ACTL_GI_DTTM"), dfOutComputed.select("ACTL_GI_DTTM"), self.maxUnequalRowsToShow)

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
