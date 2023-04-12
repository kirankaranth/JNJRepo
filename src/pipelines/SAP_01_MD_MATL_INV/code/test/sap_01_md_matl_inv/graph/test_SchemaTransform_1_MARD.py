from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_01_md_matl_inv.graph.SchemaTransform_1_MARD import *
from sap_01_md_matl_inv.config.ConfigStore import *


class SchemaTransform_1_MARDTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_matl_inv/graph/SchemaTransform_1_MARD/in0/schema.json',
            'test/resources/data/sap_01_md_matl_inv/graph/SchemaTransform_1_MARD/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_matl_inv/graph/SchemaTransform_1_MARD/out/schema.json',
            'test/resources/data/sap_01_md_matl_inv/graph/SchemaTransform_1_MARD/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = SchemaTransform_1_MARD(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("MATL_NUM", "PLNT_CD"),
            dfOutComputed.select("MATL_NUM", "PLNT_CD"),
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
