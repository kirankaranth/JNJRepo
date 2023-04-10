from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp.graph.SchemaTransform_4_MSLB import *
from sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp.config.ConfigStore import *


class SchemaTransform_4_MSLBTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp/graph/SchemaTransform_4_MSLB/in0/schema.json',
            'test/resources/data/sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp/graph/SchemaTransform_4_MSLB/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp/graph/SchemaTransform_4_MSLB/out/schema.json',
            'test/resources/data/sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp/graph/SchemaTransform_4_MSLB/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = SchemaTransform_4_MSLB(self.spark, dfIn0)
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
