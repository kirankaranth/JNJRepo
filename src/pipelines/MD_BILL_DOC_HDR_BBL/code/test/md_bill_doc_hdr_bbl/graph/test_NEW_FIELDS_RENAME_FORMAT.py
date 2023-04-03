from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from md_bill_doc_hdr_bbl.graph.NEW_FIELDS_RENAME_FORMAT import *
from md_bill_doc_hdr_bbl.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_timestamp(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_timestamp.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_timestamp.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("BILL_DOC", "CRT_DTTM", "BILL_DTTM"),
            dfOutComputed.select("BILL_DOC", "CRT_DTTM", "BILL_DTTM"),
            self.maxUnequalRowsToShow
        )

    def test_decimal(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_decimal.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_decimal.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("EXCH_RT_FIN_PSTNG", "NET_VAL_AMT"),
            dfOutComputed.select("EXCH_RT_FIN_PSTNG", "NET_VAL_AMT"),
            self.maxUnequalRowsToShow
        )

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/md_bill_doc_hdr_bbl/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_trim.json',
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
