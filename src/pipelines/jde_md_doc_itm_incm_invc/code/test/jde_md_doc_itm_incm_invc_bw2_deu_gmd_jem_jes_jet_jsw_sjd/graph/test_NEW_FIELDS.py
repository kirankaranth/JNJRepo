from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.graph.NEW_FIELDS import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_unit_test_(self):
        dfMANDT_FILTER = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd/graph/NEW_FIELDS/MANDT_FILTER/schema.json',
            'test/resources/data/jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd/graph/NEW_FIELDS/MANDT_FILTER/data/test_unit_test_.json',
            'MANDT_FILTER'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd/graph/NEW_FIELDS/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfMANDT_FILTER)
        assertDFEquals(
            dfOut.select(
              "_deleted_",
              "_pk_",
              "SRC_SYS_CD",
              "ACTG_DOC_NUM",
              "FISC_YR",
              "DOC_ITM_IN_INVC_DOC",
              "PRCHSNG_DOC_NUM",
              "REF_DOC_NUM",
              "DAI_ETL_ID",
              "DAI_CRT_DTTM",
              "DAI_UPDT_DTTM",
              "_l0_upt_",
              "_l1_upt_",
              "_pk_md5_"
            ),
            dfOutComputed.select(
              "_deleted_",
              "_pk_",
              "SRC_SYS_CD",
              "ACTG_DOC_NUM",
              "FISC_YR",
              "DOC_ITM_IN_INVC_DOC",
              "PRCHSNG_DOC_NUM",
              "REF_DOC_NUM",
              "DAI_ETL_ID",
              "DAI_CRT_DTTM",
              "DAI_UPDT_DTTM",
              "_l0_upt_",
              "_l1_upt_",
              "_pk_md5_"
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
