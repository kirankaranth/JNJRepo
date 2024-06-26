from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.graph.NEW_FIELDS_RENAME_FORMAT import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("DOC_REF_NUM", "PICK_CNTL_STS_CD"),
            dfOutComputed.select("DOC_REF_NUM", "PICK_CNTL_STS_CD"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_timestamp.json',
            'in0'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertPredicates(
            "out",
            dfOutComputed,
            list(
              zip(
                [col("EXP_DTTM").isNull(), (col("CRT_DTTM") == to_timestamp(lit("2018-02-06T02:09:16.000+0000")))],
                ["INVALID_EXP_DTTM", "VALID_CRT_DTTM"]
              )
            )
        )

    def test_decimals(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_decimals.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_decimals.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("GRS_WT_MEAS"), dfOutComputed.select("GRS_WT_MEAS"), self.maxUnequalRowsToShow)

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
