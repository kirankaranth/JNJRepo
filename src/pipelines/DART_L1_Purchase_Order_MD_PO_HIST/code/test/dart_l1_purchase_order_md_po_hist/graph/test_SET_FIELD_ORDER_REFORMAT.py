from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from dart_l1_purchase_order_md_po_hist.graph.SET_FIELD_ORDER_REFORMAT import *
from dart_l1_purchase_order_md_po_hist.config.ConfigStore import *


class SET_FIELD_ORDER_REFORMATTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/dart_l1_purchase_order_md_po_hist/graph/SET_FIELD_ORDER_REFORMAT/in0/schema.json',
            'test/resources/data/dart_l1_purchase_order_md_po_hist/graph/SET_FIELD_ORDER_REFORMAT/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/dart_l1_purchase_order_md_po_hist/graph/SET_FIELD_ORDER_REFORMAT/out/schema.json',
            'test/resources/data/dart_l1_purchase_order_md_po_hist/graph/SET_FIELD_ORDER_REFORMAT/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = SET_FIELD_ORDER_REFORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "SRC_SYS_CD",
              "PO_NUM",
              "PO_LINE_NBR",
              "PO_SEQ_NBR",
              "EV_TYPE_CO",
              "MATL_MVMT_NUM",
              "MATL_MVMT_SEQ_NBR",
              "UNIQ_KEY_ID"
            ),
            dfOutComputed.select(
              "SRC_SYS_CD",
              "PO_NUM",
              "PO_LINE_NBR",
              "PO_SEQ_NBR",
              "EV_TYPE_CO",
              "MATL_MVMT_NUM",
              "MATL_MVMT_SEQ_NBR",
              "UNIQ_KEY_ID"
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
