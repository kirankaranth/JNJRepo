from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.graph.NEW_FIELDS_TRANSFORMATION import *
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.config.ConfigStore import *


class NEW_FIELDS_TRANSFORMATIONTest(BaseTestCase):

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd/graph/NEW_FIELDS_TRANSFORMATION/in0/schema.json',
            'test/resources/data/jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd/graph/NEW_FIELDS_TRANSFORMATION/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd/graph/NEW_FIELDS_TRANSFORMATION/out/schema.json',
            'test/resources/data/jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd/graph/NEW_FIELDS_TRANSFORMATION/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_TRANSFORMATION(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("CO_CD", "CUST_NUM"),
            dfOutComputed.select("CO_CD", "CUST_NUM"),
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
