from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_01_md_plnt.graph.NEW_FIELDS_RENAME_FORMAT import *
from sap_01_md_plnt.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_pk_and_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_plnt/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_01_md_plnt/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_pk_and_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_plnt/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_01_md_plnt/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_pk_and_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("PLNT_CD", "PLNT_NM"),
            dfOutComputed.select("PLNT_CD", "PLNT_NM"),
            self.maxUnequalRowsToShow
        )

    def test_country_code(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_plnt/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_01_md_plnt/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_country_code.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_plnt/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_01_md_plnt/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_country_code.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CTRY_CD"), dfOutComputed.select("CTRY_CD"), self.maxUnequalRowsToShow)

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
        dfgraph_LU_SAP_T005T = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_plnt/graph/LU_SAP_T005T/schema.json',
            'test/resources/data/sap_01_md_plnt/graph/LU_SAP_T005T/data.json',
            "in0"
        )
        from sap_01_md_plnt.graph.LU_SAP_T005T import LU_SAP_T005T
        LU_SAP_T005T(self.spark, dfgraph_LU_SAP_T005T)
