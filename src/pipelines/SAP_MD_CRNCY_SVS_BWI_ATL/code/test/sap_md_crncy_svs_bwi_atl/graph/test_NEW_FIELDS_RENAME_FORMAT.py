from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_crncy_svs_bwi_atl.graph.NEW_FIELDS_RENAME_FORMAT import *
from sap_md_crncy_svs_bwi_atl.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_crncy_svs_bwi_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_crncy_svs_bwi_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_crncy_svs_bwi_atl/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_crncy_svs_bwi_atl/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("ISO_CRNCY_CD", "CRNCY_ALT_CD"),
            dfOutComputed.select("ISO_CRNCY_CD", "CRNCY_ALT_CD"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_crncy_svs_bwi_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_crncy_svs_bwi_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_timestamp.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_crncy_svs_bwi_atl/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_crncy_svs_bwi_atl/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_timestamp.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("VLD_TO_DTTM"), dfOutComputed.select("VLD_TO_DTTM"), self.maxUnequalRowsToShow)

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
