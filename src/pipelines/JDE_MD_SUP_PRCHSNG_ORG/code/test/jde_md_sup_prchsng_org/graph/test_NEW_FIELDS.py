from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_sup_prchsng_org.graph.NEW_FIELDS import *
from jde_md_sup_prchsng_org.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("CRNCY_CD", "INCOTERM1_CD", "INCOTERM2_CD"),
            dfOutComputed.select("CRNCY_CD", "INCOTERM1_CD", "INCOTERM2_CD"),
            self.maxUnequalRowsToShow
        )

    def test_pk(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/in0/data/test_pk.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/out/data/test_pk.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("SRC_SYS_CD", "SUP_NUM", "PRCHSNG_ORG_NUM"),
            dfOutComputed.select("SRC_SYS_CD", "SUP_NUM", "PRCHSNG_ORG_NUM"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/in0/data/test_timestamp_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_md_sup_prchsng_org/graph/NEW_FIELDS/out/data/test_timestamp_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CRT_ON_DTTM"), dfOutComputed.select("CRT_ON_DTTM"), self.maxUnequalRowsToShow)

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
