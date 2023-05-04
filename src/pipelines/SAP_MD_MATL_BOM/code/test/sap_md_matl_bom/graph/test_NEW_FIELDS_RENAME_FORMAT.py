from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_bom.graph.NEW_FIELDS_RENAME_FORMAT import *
from sap_md_matl_bom.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_bom/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_matl_bom/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_bom/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_matl_bom/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "MATL_NUM",
              "PLNT_CD",
              "BOM_USG_CD",
              "BOM_NUM",
              "ALT_BOM_NUM",
              "FROM_LOT_SIZE_QTY",
              "TO_LOT_SIZE_QTY",
              "USER_WHO_CRT_REC",
              "NM_OF_PRSN_WHO_CHG_OBJ",
              "CNFG_MATL_IN"
            ),
            dfOutComputed.select(
              "MATL_NUM",
              "PLNT_CD",
              "BOM_USG_CD",
              "BOM_NUM",
              "ALT_BOM_NUM",
              "FROM_LOT_SIZE_QTY",
              "TO_LOT_SIZE_QTY",
              "USER_WHO_CRT_REC",
              "NM_OF_PRSN_WHO_CHG_OBJ",
              "CNFG_MATL_IN"
            ),
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
