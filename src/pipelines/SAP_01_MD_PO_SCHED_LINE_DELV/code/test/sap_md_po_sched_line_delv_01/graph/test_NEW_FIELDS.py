from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_po_sched_line_delv_01.graph.NEW_FIELDS import *
from sap_md_po_sched_line_delv_01.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_po_sched_line_delv_01/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_po_sched_line_delv_01/graph/NEW_FIELDS/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_po_sched_line_delv_01/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_po_sched_line_delv_01/graph/NEW_FIELDS/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("DELV_DTTM", "STAT_DELV_DTTM", "CMT_DTTM"),
            dfOutComputed.select("DELV_DTTM", "STAT_DELV_DTTM", "CMT_DTTM"),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_po_sched_line_delv_01/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_po_sched_line_delv_01/graph/NEW_FIELDS/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_po_sched_line_delv_01/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_po_sched_line_delv_01/graph/NEW_FIELDS/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "SCHD_QTY",
              "RECV_QTY",
              "STK_TFR_RECV_QTY",
              "MRP_ADJ_QTY",
              "CMT_QTY",
              "PREV_QTY",
              "CAT_OF_DELV_DT",
              "BTCH_NUM"
            ),
            dfOutComputed.select(
              "SCHD_QTY",
              "RECV_QTY",
              "STK_TFR_RECV_QTY",
              "MRP_ADJ_QTY",
              "CMT_QTY",
              "PREV_QTY",
              "CAT_OF_DELV_DT",
              "BTCH_NUM"
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
