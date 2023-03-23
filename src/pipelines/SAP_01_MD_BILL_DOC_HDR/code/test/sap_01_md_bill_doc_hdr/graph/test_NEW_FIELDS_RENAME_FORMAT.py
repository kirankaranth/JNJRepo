from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_01_md_bill_doc_hdr.graph.NEW_FIELDS_RENAME_FORMAT import *
import sap_01_md_bill_doc_hdr.config.ConfigStore as ConfigStore


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_01_md_bill_doc_hdr/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "BILL_DOC",
              "SLORG_DESC",
              "DSTR_CHNL_DESC",
              "SLS_DIV_DESC",
              "BILL_TYPE_DESC",
              "CRT_DTTM",
              "BILL_DTTM",
              "INDSTR_CD_1",
              "BILL_DOC_IS_CAN",
              "EXCH_RT_FIN_PSTNG"
            ),
            dfOutComputed.select(
              "BILL_DOC",
              "SLORG_DESC",
              "DSTR_CHNL_DESC",
              "SLS_DIV_DESC",
              "BILL_TYPE_DESC",
              "CRT_DTTM",
              "BILL_DTTM",
              "INDSTR_CD_1",
              "BILL_DOC_IS_CAN",
              "EXCH_RT_FIN_PSTNG"
            ),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        ConfigStore.Utils.initializeFromArgs(
            self.spark,
            Namespace(file = f"configs/resources/config/{fabricName}.json", config = None, overrideJson = None)
        )
