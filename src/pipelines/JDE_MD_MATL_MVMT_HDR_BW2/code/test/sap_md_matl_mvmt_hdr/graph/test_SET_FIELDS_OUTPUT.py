from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_mvmt_hdr.graph.SET_FIELDS_OUTPUT import *
from sap_md_matl_mvmt_hdr.config.ConfigStore import *


class SET_FIELDS_OUTPUTTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_mvmt_hdr/graph/SET_FIELDS_OUTPUT/in0/schema.json',
            'test/resources/data/sap_md_matl_mvmt_hdr/graph/SET_FIELDS_OUTPUT/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_mvmt_hdr/graph/SET_FIELDS_OUTPUT/out/schema.json',
            'test/resources/data/sap_md_matl_mvmt_hdr/graph/SET_FIELDS_OUTPUT/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = SET_FIELDS_OUTPUT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("MATL_MVMT_DTTM", "PSTNG_DTTM", "LAST_CHG_DTTM"),
            dfOutComputed.select("MATL_MVMT_DTTM", "PSTNG_DTTM", "LAST_CHG_DTTM"),
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
