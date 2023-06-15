from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_mfg_order_hcs.graph.XFORM import *
from sap_md_mfg_order_hcs.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_datetime_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/in0/data/test_datetime_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/out/data/test_datetime_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CHG_DTTM"), dfOutComputed.select("CHG_DTTM"), self.maxUnequalRowsToShow)

    def test_decimal_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/in0/data/test_decimal_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/out/data/test_decimal_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("EST_TOT_COSTS_OF_ORDR"),
            dfOutComputed.select("EST_TOT_COSTS_OF_ORDR"),
            self.maxUnequalRowsToShow
        )

    def test_trim_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/in0/data/test_trim_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_mfg_order_hcs/graph/XFORM/out/data/test_trim_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("MFG_ORDR_TYP_CD", "MFG_ORDR_NUM", "DEL_IND", "OBJECT_NUMBER", "PLNT_CD"),
            dfOutComputed.select("MFG_ORDR_TYP_CD", "MFG_ORDR_NUM", "DEL_IND", "OBJECT_NUMBER", "PLNT_CD"),
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
