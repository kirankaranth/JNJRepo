from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from dart_l1_purchase_order_md_po_hist.graph.NEW_FIELDS import *
from dart_l1_purchase_order_md_po_hist.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/dart_l1_purchase_order_md_po_hist/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/dart_l1_purchase_order_md_po_hist/graph/NEW_FIELDS/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/dart_l1_purchase_order_md_po_hist/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/dart_l1_purchase_order_md_po_hist/graph/NEW_FIELDS/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("ZEKKN", "VGABE", "GJAHR", "EBELP", "EBELN", "BUZEI", "BELNR"),
            dfOutComputed.select("ZEKKN", "VGABE", "GJAHR", "EBELP", "EBELN", "BUZEI", "BELNR"),
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
