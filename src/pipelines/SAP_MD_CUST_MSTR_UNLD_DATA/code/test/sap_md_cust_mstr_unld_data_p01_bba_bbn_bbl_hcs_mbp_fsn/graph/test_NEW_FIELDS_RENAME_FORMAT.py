from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.graph.NEW_FIELDS_RENAME_FORMAT import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_trim_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_trim_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_trim_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("CUST_FCTRY_CAL"),
            dfOutComputed.select("CUST_FCTRY_CAL"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_timestamp_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_timestamp_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("_l0_upt_"), dfOutComputed.select("_l0_upt_"), self.maxUnequalRowsToShow)

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
