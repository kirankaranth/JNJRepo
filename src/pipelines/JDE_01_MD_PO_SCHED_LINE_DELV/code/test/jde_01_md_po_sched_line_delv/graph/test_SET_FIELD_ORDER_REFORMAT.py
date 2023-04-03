from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_01_md_po_sched_line_delv.graph.SET_FIELD_ORDER_REFORMAT import *
from jde_01_md_po_sched_line_delv.config.ConfigStore import *


class SET_FIELD_ORDER_REFORMATTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_po_sched_line_delv/graph/SET_FIELD_ORDER_REFORMAT/in0/schema.json',
            'test/resources/data/jde_01_md_po_sched_line_delv/graph/SET_FIELD_ORDER_REFORMAT/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOutComputed = SET_FIELD_ORDER_REFORMAT(self.spark, dfIn0)

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
