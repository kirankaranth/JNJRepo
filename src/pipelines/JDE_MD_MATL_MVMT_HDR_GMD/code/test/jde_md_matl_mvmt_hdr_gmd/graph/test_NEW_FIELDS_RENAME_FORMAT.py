from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_matl_mvmt_hdr_gmd.graph.NEW_FIELDS_RENAME_FORMAT import *
from jde_md_matl_mvmt_hdr_gmd.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_mvmt_hdr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/jde_md_matl_mvmt_hdr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_mvmt_hdr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/jde_md_matl_mvmt_hdr_gmd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("LAST_CHG_DTTM", "MATL_MVMT_DTTM", "PSTNG_DTTM"),
            dfOutComputed.select("LAST_CHG_DTTM", "MATL_MVMT_DTTM", "PSTNG_DTTM"),
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
