from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_02_md_plnt_mtr.graph.NEW_FIELDS_RENAME_FORMAT import *
from jde_02_md_plnt_mtr.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_pk(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_02_md_plnt_mtr/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/jde_02_md_plnt_mtr/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_pk.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_02_md_plnt_mtr/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/jde_02_md_plnt_mtr/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_pk.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("SRC_SYS_CD", "PLNT_CD"),
            dfOutComputed.select("SRC_SYS_CD", "PLNT_CD"),
            self.maxUnequalRowsToShow
        )

    def test_trim_country___company_code(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_02_md_plnt_mtr/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/jde_02_md_plnt_mtr/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_trim_country___company_code.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_02_md_plnt_mtr/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/jde_02_md_plnt_mtr/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_trim_country___company_code.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("CTRY_CD", "CO_CD"),
            dfOutComputed.select("CTRY_CD", "CO_CD"),
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
