from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_sls_ordr_line_.graph.SET_FIELDS_REFORMAT_ORDER import *
from jde_md_sls_ordr_line_.config.ConfigStore import *


class SET_FIELDS_REFORMAT_ORDERTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_ordr_line_/graph/SET_FIELDS_REFORMAT_ORDER/in0/schema.json',
            'test/resources/data/jde_md_sls_ordr_line_/graph/SET_FIELDS_REFORMAT_ORDER/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_ordr_line_/graph/SET_FIELDS_REFORMAT_ORDER/out/schema.json',
            'test/resources/data/jde_md_sls_ordr_line_/graph/SET_FIELDS_REFORMAT_ORDER/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = SET_FIELDS_REFORMAT_ORDER(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("CR_DTTM", "AVAIL_TO_PROM_DTTM", "ORIG_RQST_DELV_DTTM", "CHG_DTTM"),
            dfOutComputed.select("CR_DTTM", "AVAIL_TO_PROM_DTTM", "ORIG_RQST_DELV_DTTM", "CHG_DTTM"),
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
