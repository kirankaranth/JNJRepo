from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sk_sap_01_md_ser_num_stock_sgmnt.graph.NEW_FIELDS_RENAME_FORMAT import *
import sk_sap_01_md_ser_num_stock_sgmnt.config.ConfigStore as ConfigStore


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sk_sap_01_md_ser_num_stock_sgmnt/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sk_sap_01_md_ser_num_stock_sgmnt/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sk_sap_01_md_ser_num_stock_sgmnt/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sk_sap_01_md_ser_num_stock_sgmnt/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "EQMNT_NUM",
              "STOCK_TYPE_GOODS_MVMT",
              "PLNT",
              "STRG_LOC",
              "BTCH_NUM",
              "SPL_STOCK_IN",
              "SPL_STOCK_CUST_ACCT_NUM",
              "ACCT_NUM_VEND",
              "SLS_ORDR_NUM",
              "ITM_NUM_SLS_ORDR",
              "WBS_ELMNT",
              "OWN_STOCK"
            ),
            dfOutComputed.select(
              "EQMNT_NUM",
              "STOCK_TYPE_GOODS_MVMT",
              "PLNT",
              "STRG_LOC",
              "BTCH_NUM",
              "SPL_STOCK_IN",
              "SPL_STOCK_CUST_ACCT_NUM",
              "ACCT_NUM_VEND",
              "SLS_ORDR_NUM",
              "ITM_NUM_SLS_ORDR",
              "WBS_ELMNT",
              "OWN_STOCK"
            ),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        ConfigStore.Utils.initializeFromArgs(
            self.spark,
            Namespace(file = f"configs/resources/config/{fabricName}.json", config = None, overrideJson = None)
        )
