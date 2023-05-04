from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_loc_bba_bbl_bbn.graph.SET_FIELD_ORDER_REFORMAT import *
from sap_md_matl_loc_bba_bbl_bbn.config.ConfigStore import *


class SET_FIELD_ORDER_REFORMATTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_loc_bba_bbl_bbn/graph/SET_FIELD_ORDER_REFORMAT/in0/schema.json',
            'test/resources/data/sap_md_matl_loc_bba_bbl_bbn/graph/SET_FIELD_ORDER_REFORMAT/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_loc_bba_bbl_bbn/graph/SET_FIELD_ORDER_REFORMAT/out/schema.json',
            'test/resources/data/sap_md_matl_loc_bba_bbl_bbn/graph/SET_FIELD_ORDER_REFORMAT/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = SET_FIELD_ORDER_REFORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("SPEC_MATL_PLNT_STS_DTTM"),
            dfOutComputed.select("SPEC_MATL_PLNT_STS_DTTM"),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_loc_bba_bbl_bbn/graph/SET_FIELD_ORDER_REFORMAT/in0/schema.json',
            'test/resources/data/sap_md_matl_loc_bba_bbl_bbn/graph/SET_FIELD_ORDER_REFORMAT/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_loc_bba_bbl_bbn/graph/SET_FIELD_ORDER_REFORMAT/out/schema.json',
            'test/resources/data/sap_md_matl_loc_bba_bbl_bbn/graph/SET_FIELD_ORDER_REFORMAT/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = SET_FIELD_ORDER_REFORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("VAR_CNTL_CD"), dfOutComputed.select("VAR_CNTL_CD"), self.maxUnequalRowsToShow)

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
