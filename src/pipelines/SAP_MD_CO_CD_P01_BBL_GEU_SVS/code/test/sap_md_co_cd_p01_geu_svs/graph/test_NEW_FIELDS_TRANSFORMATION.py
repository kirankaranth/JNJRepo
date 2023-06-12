from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_co_cd_p01_geu_svs.graph.NEW_FIELDS_TRANSFORMATION import *
from sap_md_co_cd_p01_geu_svs.config.ConfigStore import *


class NEW_FIELDS_TRANSFORMATIONTest(BaseTestCase):

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/in0/schema.json',
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/out/schema.json',
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_TRANSFORMATION(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("TERM_PMT_KEY", "RCNL_ACCT_GENL_LDGR"),
            dfOutComputed.select("TERM_PMT_KEY", "RCNL_ACCT_GENL_LDGR"),
            self.maxUnequalRowsToShow
        )

    def test_decimal(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/in0/schema.json',
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/in0/data/test_decimal.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/out/schema.json',
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/out/data/test_decimal.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_TRANSFORMATION(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("AMT_INS", "BILL_EXCH_LMT"),
            dfOutComputed.select("AMT_INS", "BILL_EXCH_LMT"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/in0/schema.json',
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/in0/data/test_timestamp.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/out/schema.json',
            'test/resources/data/sap_md_co_cd_p01_geu_svs/graph/NEW_FIELDS_TRANSFORMATION/out/data/test_timestamp.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_TRANSFORMATION(self.spark, dfIn0)
        assertDFEquals(dfOut.select("REC_CRT_DTTM"), dfOutComputed.select("REC_CRT_DTTM"), self.maxUnequalRowsToShow)

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
