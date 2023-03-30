from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_01_md_matl_loc.graph.SET_FIELDS_ORDER_REFORMAT import *
from sap_01_md_matl_loc.config.ConfigStore import *


class SET_FIELDS_ORDER_REFORMATTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/in0/schema.json',
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/out/schema.json',
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = SET_FIELDS_ORDER_REFORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("SRC_SYS_CD", "MATL_NUM", "PLNT_CD"),
            dfOutComputed.select("SRC_SYS_CD", "MATL_NUM", "PLNT_CD"),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/in0/schema.json',
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/out/schema.json',
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = SET_FIELDS_ORDER_REFORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("REORDR_QTY"), dfOutComputed.select("REORDR_QTY"), self.maxUnequalRowsToShow)

    def test_unit_test_2(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/in0/schema.json',
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/in0/data/test_unit_test_2.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/out/schema.json',
            'test/resources/data/sap_01_md_matl_loc/graph/SET_FIELDS_ORDER_REFORMAT/out/data/test_unit_test_2.json',
            'out'
        )
        dfOutComputed = SET_FIELDS_ORDER_REFORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CUR_PER"), dfOutComputed.select("CUR_PER"), self.maxUnequalRowsToShow)

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
