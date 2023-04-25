from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_01_md_sls_ordr_line_hmd.graph.NEW_FIEDS import *
from sap_01_md_sls_ordr_line_hmd.config.ConfigStore import *


class NEW_FIEDSTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_line_hmd/graph/NEW_FIEDS/in0/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_line_hmd/graph/NEW_FIEDS/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_line_hmd/graph/NEW_FIEDS/out/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_line_hmd/graph/NEW_FIEDS/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIEDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("ORG_MTL_AVAL_DTTM", "CUST_PO_DTTM"),
            dfOutComputed.select("ORG_MTL_AVAL_DTTM", "CUST_PO_DTTM"),
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
