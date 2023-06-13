from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_btch.graph.SchemaTransform_MCH1 import *
from sap_md_btch.config.ConfigStore import *


class SchemaTransform_MCH1Test(BaseTestCase):

    def test_pk(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/in0/schema.json',
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/in0/data/test_pk.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/out/schema.json',
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/out/data/test_pk.json',
            'out'
        )
        dfOutComputed = SchemaTransform_MCH1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("SRC_SYS_CD", "SRC_TBL_NM", "MATL_NUM", "BTCH_NUM", "SHRT_MATL_NUM"),
            dfOutComputed.select("SRC_SYS_CD", "SRC_TBL_NM", "MATL_NUM", "BTCH_NUM", "SHRT_MATL_NUM"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/in0/schema.json',
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/in0/data/test_timestamp.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/out/schema.json',
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/out/data/test_timestamp.json',
            'out'
        )
        dfOutComputed = SchemaTransform_MCH1(self.spark, dfIn0)
        assertDFEquals(dfOut.select("AVAIL_DTTM"), dfOutComputed.select("AVAIL_DTTM"), self.maxUnequalRowsToShow)

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/in0/schema.json',
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/out/schema.json',
            'test/resources/data/sap_md_btch/graph/SchemaTransform_MCH1/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = SchemaTransform_MCH1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("DEL_IND", "SUP_NUM", "SUP_BTCH_NUM"),
            dfOutComputed.select("DEL_IND", "SUP_NUM", "SUP_BTCH_NUM"),
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
