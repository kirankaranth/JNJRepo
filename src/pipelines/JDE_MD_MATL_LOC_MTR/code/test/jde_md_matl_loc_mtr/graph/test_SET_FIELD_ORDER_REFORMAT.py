from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_matl_loc_mtr.graph.SET_FIELD_ORDER_REFORMAT import *
from jde_md_matl_loc_mtr.config.ConfigStore import *


class SET_FIELD_ORDER_REFORMATTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_loc_mtr/graph/SET_FIELD_ORDER_REFORMAT/in0/schema.json',
            'test/resources/data/jde_md_matl_loc_mtr/graph/SET_FIELD_ORDER_REFORMAT/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_loc_mtr/graph/SET_FIELD_ORDER_REFORMAT/out/schema.json',
            'test/resources/data/jde_md_matl_loc_mtr/graph/SET_FIELD_ORDER_REFORMAT/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = SET_FIELD_ORDER_REFORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "SPEC_MATL_PLNT_STS_CD",
              "LOT_SIZE_VAL",
              "MATL_PLNR_NUM",
              "MATL_ABC_CLSN_CD",
              "AVLBLTY_CHK_IND",
              "PRCMT_TYPE_CD",
              "MRP_TYPE_CD",
              "ORIG_CTRY_CD",
              "RD_VAL_QTY",
              "LOT_SIZE_FX_QTY",
              "LOT_SIZE_MAX_QTY",
              "LOT_SIZE_MIN_QTY",
              "SFTY_STK_QTY",
              "PLNG_TIME_FENCE_DAYS_CNT",
              "PLAN_DELV_DAYS_CNT",
              "INHS_PRDTN_DAYS_CNT",
              "MSTR_PLNG_FMLY_CD"
            ),
            dfOutComputed.select(
              "SPEC_MATL_PLNT_STS_CD",
              "LOT_SIZE_VAL",
              "MATL_PLNR_NUM",
              "MATL_ABC_CLSN_CD",
              "AVLBLTY_CHK_IND",
              "PRCMT_TYPE_CD",
              "MRP_TYPE_CD",
              "ORIG_CTRY_CD",
              "RD_VAL_QTY",
              "LOT_SIZE_FX_QTY",
              "LOT_SIZE_MAX_QTY",
              "LOT_SIZE_MIN_QTY",
              "SFTY_STK_QTY",
              "PLNG_TIME_FENCE_DAYS_CNT",
              "PLAN_DELV_DAYS_CNT",
              "INHS_PRDTN_DAYS_CNT",
              "MSTR_PLNG_FMLY_CD"
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
