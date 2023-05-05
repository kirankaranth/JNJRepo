from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.graph.NEW_FIELDS import *
from sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_unit_test_(self):
        dfMANDT_FILTER = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai/graph/NEW_FIELDS/MANDT_FILTER/schema.json',
            'test/resources/data/sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai/graph/NEW_FIELDS/MANDT_FILTER/data/test_unit_test_.json',
            'MANDT_FILTER'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai/graph/NEW_FIELDS/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfMANDT_FILTER)
        assertDFEquals(
            dfOut.select(
              "SRC_SYS_CD",
              "ACTG_DOC_NUM",
              "FISC_YR",
              "DOC_ITM_IN_INVC_DOC",
              "PRCHSNG_DOC_NUM",
              "SEQ_NUM_OF_ACCT_ASGNMT",
              "MATL_NUM",
              "VALUT_AREA"
            ),
            dfOutComputed.select(
              "SRC_SYS_CD",
              "ACTG_DOC_NUM",
              "FISC_YR",
              "DOC_ITM_IN_INVC_DOC",
              "PRCHSNG_DOC_NUM",
              "SEQ_NUM_OF_ACCT_ASGNMT",
              "MATL_NUM",
              "VALUT_AREA"
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
