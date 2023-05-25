from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_01_md_sls_ordr_bwi.graph.NEW_FIELDS_TRANSFORMATION import *
from sap_01_md_sls_ordr_bwi.config.ConfigStore import *


class NEW_FIELDS_TRANSFORMATIONTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/NEW_FIELDS_TRANSFORMATION/in0/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/NEW_FIELDS_TRANSFORMATION/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/NEW_FIELDS_TRANSFORMATION/out/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/NEW_FIELDS_TRANSFORMATION/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_TRANSFORMATION(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("COMPANY_CD", "SLS_ORDR_DOC_ID", "SLS_ORDR_TYPE_CD"),
            dfOutComputed.select("COMPANY_CD", "SLS_ORDR_DOC_ID", "SLS_ORDR_TYPE_CD"),
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
        dfgraph_LU_SAP_T001 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_T001/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_T001/data.json',
            "in0"
        )
        from sap_01_md_sls_ordr_bwi.graph.LU_SAP_T001 import LU_SAP_T001
        LU_SAP_T001(self.spark, dfgraph_LU_SAP_T001)
        dfgraph_LU_SAP_TVTWT = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVTWT/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVTWT/data.json',
            "in0"
        )
        from sap_01_md_sls_ordr_bwi.graph.LU_SAP_TVTWT import LU_SAP_TVTWT
        LU_SAP_TVTWT(self.spark, dfgraph_LU_SAP_TVTWT)
        dfgraph_LU_SAP_TVFST = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVFST/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVFST/data.json',
            "in0"
        )
        from sap_01_md_sls_ordr_bwi.graph.LU_SAP_TVFST import LU_SAP_TVFST
        LU_SAP_TVFST(self.spark, dfgraph_LU_SAP_TVFST)
        dfgraph_LU_SAP_TVAKT = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVAKT/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVAKT/data.json',
            "in0"
        )
        from sap_01_md_sls_ordr_bwi.graph.LU_SAP_TVAKT import LU_SAP_TVAKT
        LU_SAP_TVAKT(self.spark, dfgraph_LU_SAP_TVAKT)
        dfgraph_LU_SAP_TVKOT = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVKOT/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVKOT/data.json',
            "in0"
        )
        from sap_01_md_sls_ordr_bwi.graph.LU_SAP_TVKOT import LU_SAP_TVKOT
        LU_SAP_TVKOT(self.spark, dfgraph_LU_SAP_TVKOT)
        dfgraph_LU_SAP_T176T = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_T176T/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_T176T/data.json',
            "in0"
        )
        from sap_01_md_sls_ordr_bwi.graph.LU_SAP_T176T import LU_SAP_T176T
        LU_SAP_T176T(self.spark, dfgraph_LU_SAP_T176T)
        dfgraph_LU_SAP_TVLST = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVLST/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TVLST/data.json',
            "in0"
        )
        from sap_01_md_sls_ordr_bwi.graph.LU_SAP_TVLST import LU_SAP_TVLST
        LU_SAP_TVLST(self.spark, dfgraph_LU_SAP_TVLST)
        dfgraph_LU_SAP_TSPAT = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TSPAT/schema.json',
            'test/resources/data/sap_01_md_sls_ordr_bwi/graph/LU_SAP_TSPAT/data.json',
            "in0"
        )
        from sap_01_md_sls_ordr_bwi.graph.LU_SAP_TSPAT import LU_SAP_TSPAT
        LU_SAP_TSPAT(self.spark, dfgraph_LU_SAP_TSPAT)
