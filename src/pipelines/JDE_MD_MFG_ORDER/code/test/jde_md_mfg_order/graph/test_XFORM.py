from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_mfg_order.graph.XFORM import *
from jde_md_mfg_order.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_decimal_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order/graph/XFORM/in0/schema.json',
            'test/resources/data/jde_md_mfg_order/graph/XFORM/in0/data/test_decimal_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order/graph/XFORM/out/schema.json',
            'test/resources/data/jde_md_mfg_order/graph/XFORM/out/data/test_decimal_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("CNFRMD_SCRP_QTY"),
            dfOutComputed.select("CNFRMD_SCRP_QTY"),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order/graph/XFORM/in0/schema.json',
            'test/resources/data/jde_md_mfg_order/graph/XFORM/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order/graph/XFORM/out/schema.json',
            'test/resources/data/jde_md_mfg_order/graph/XFORM/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("MFG_ORDR_TYP_CD", "MFG_ORDR_NUM", "RTNG_TYP_CD", "MFG_ORDR_STTS_CD"),
            dfOutComputed.select("MFG_ORDR_TYP_CD", "MFG_ORDR_NUM", "RTNG_TYP_CD", "MFG_ORDR_STTS_CD"),
            self.maxUnequalRowsToShow
        )

    def test_date_time_test(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order/graph/XFORM/in0/schema.json',
            'test/resources/data/jde_md_mfg_order/graph/XFORM/in0/data/test_date_time_test.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_mfg_order/graph/XFORM/out/schema.json',
            'test/resources/data/jde_md_mfg_order/graph/XFORM/out/data/test_date_time_test.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CHG_DTTM"), dfOutComputed.select("CHG_DTTM"), self.maxUnequalRowsToShow)

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
