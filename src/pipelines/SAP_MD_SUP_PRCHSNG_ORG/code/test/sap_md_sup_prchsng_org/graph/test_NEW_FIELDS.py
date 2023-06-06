from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_sup_prchsng_org.graph.NEW_FIELDS import *
from sap_md_sup_prchsng_org.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_trim_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/in0/data/test_trim_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/out/data/test_trim_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "PRCH_BLK_IND",
              "DEL_IND",
              "CRNCY_CD",
              "PMT_TERM_CD",
              "INCOTERM1_CD",
              "INCOTERM2_CD",
              "PRC_PCDR_CD",
              "PRC_CNTL_CD",
              "EVAL_RCPT_SETLM_CD",
              "RTRN_VEND_IND",
              "CNFRM_CD",
              "NM_OF_PRSN_RESP_CREAT_OBJ",
              "GR_BAS_INVC_VERIF",
              "AUTO_GNR_OF_PO_ALLW",
              "AUTO_EVAL_RCPT_SETLM"
            ),
            dfOutComputed.select(
              "PRCH_BLK_IND",
              "DEL_IND",
              "CRNCY_CD",
              "PMT_TERM_CD",
              "INCOTERM1_CD",
              "INCOTERM2_CD",
              "PRC_PCDR_CD",
              "PRC_CNTL_CD",
              "EVAL_RCPT_SETLM_CD",
              "RTRN_VEND_IND",
              "CNFRM_CD",
              "NM_OF_PRSN_RESP_CREAT_OBJ",
              "GR_BAS_INVC_VERIF",
              "AUTO_GNR_OF_PO_ALLW",
              "AUTO_EVAL_RCPT_SETLM"
            ),
            self.maxUnequalRowsToShow
        )

    def test_pk(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/in0/data/test_pk.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/out/data/test_pk.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("SRC_SYS_CD", "SUP_NUM", "PRCHSNG_ORG_NUM"),
            dfOutComputed.select("SRC_SYS_CD", "SUP_NUM", "PRCHSNG_ORG_NUM"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/in0/data/test_timestamp.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_sup_prchsng_org/graph/NEW_FIELDS/out/data/test_timestamp.json',
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
