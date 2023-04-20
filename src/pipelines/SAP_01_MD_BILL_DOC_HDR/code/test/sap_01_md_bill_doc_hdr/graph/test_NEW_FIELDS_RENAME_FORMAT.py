from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_01_md_bill_doc_hdr.graph.NEW_FIELDS_RENAME_FORMAT import *
from sap_01_md_bill_doc_hdr.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_decimal_test(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_decimal_test.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_decimal_test.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("EXCH_RT_FIN_PSTNG", "NET_VAL_AMT"),
            dfOutComputed.select("EXCH_RT_FIN_PSTNG", "NET_VAL_AMT"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_timestamp_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_timestamp_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("BILL_DOC", "CRT_DTTM", "BILL_DTTM"),
            dfOutComputed.select("BILL_DOC", "CRT_DTTM", "BILL_DTTM"),
            self.maxUnequalRowsToShow
        )

    def test_trimtest_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_trimtest_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_trimtest_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("SLORG_CD"), dfOutComputed.select("SLORG_CD"), self.maxUnequalRowsToShow)

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
