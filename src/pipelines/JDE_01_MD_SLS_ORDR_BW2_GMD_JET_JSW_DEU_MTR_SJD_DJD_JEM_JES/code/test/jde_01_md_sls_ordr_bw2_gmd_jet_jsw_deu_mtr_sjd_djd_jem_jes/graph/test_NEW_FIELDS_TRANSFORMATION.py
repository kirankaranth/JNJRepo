from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.graph.NEW_FIELDS_TRANSFORMATION import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *


class NEW_FIELDS_TRANSFORMATIONTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS_TRANSFORMATION/in0/schema.json',
            'test/resources/data/jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS_TRANSFORMATION/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS_TRANSFORMATION/out/schema.json',
            'test/resources/data/jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes/graph/NEW_FIELDS_TRANSFORMATION/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_TRANSFORMATION(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("COMPANY_CD", "SLS_ORDR_DOC_ID", "SLS_ORDR_TYPE_CD"),
            dfOutComputed.select("COMPANY_CD", "SLS_ORDR_DOC_ID", "SLS_ORDR_TYPE_CD"),
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
