from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_own_expln_for_term_of_pmt.graph.XFORM import *
from sap_md_own_expln_for_term_of_pmt.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_own_expln_for_term_of_pmt/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_own_expln_for_term_of_pmt/graph/XFORM/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_own_expln_for_term_of_pmt/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_own_expln_for_term_of_pmt/graph/XFORM/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("OWN_EXPLN_OF_TERM_OF_PMT"),
            dfOutComputed.select("OWN_EXPLN_OF_TERM_OF_PMT"),
            self.maxUnequalRowsToShow
        )

    def test_int_test(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_own_expln_for_term_of_pmt/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_own_expln_for_term_of_pmt/graph/XFORM/in0/data/test_int_test.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_own_expln_for_term_of_pmt/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_own_expln_for_term_of_pmt/graph/XFORM/out/data/test_int_test.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("DAY_LMT"), dfOutComputed.select("DAY_LMT"), self.maxUnequalRowsToShow)

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
