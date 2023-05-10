from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.graph.NEW_FIELDS_01_T006 import *
from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.config.ConfigStore import *


class NEW_FIELDS_01_T006Test(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/NEW_FIELDS_01_T006/in0/schema.json',
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/NEW_FIELDS_01_T006/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/NEW_FIELDS_01_T006/out/schema.json',
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/NEW_FIELDS_01_T006/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_01_T006(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("UOM_CD", "EXTRNL_UOM_IN", "DENOM_FOR_CONV_UNIT", "DIM_KEY"),
            dfOutComputed.select("UOM_CD", "EXTRNL_UOM_IN", "DENOM_FOR_CONV_UNIT", "DIM_KEY"),
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
        dfgraph_Lookup_02_T006A = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/Lookup_02_T006A/schema.json',
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/Lookup_02_T006A/data.json',
            "in0"
        )
        from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.graph.Lookup_02_T006A import Lookup_02_T006A
        Lookup_02_T006A(self.spark, dfgraph_Lookup_02_T006A)
        dfgraph_Lookup_03_T006A = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/Lookup_03_T006A/schema.json',
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/Lookup_03_T006A/data.json',
            "in0"
        )
        from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.graph.Lookup_03_T006A import Lookup_03_T006A
        Lookup_03_T006A(self.spark, dfgraph_Lookup_03_T006A)
        dfgraph_Lookup_01_T006A = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/Lookup_01_T006A/schema.json',
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/Lookup_01_T006A/data.json',
            "in0"
        )
        from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.graph.Lookup_01_T006A import Lookup_01_T006A
        Lookup_01_T006A(self.spark, dfgraph_Lookup_01_T006A)
        dfgraph_Lookup_04_T006A = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/Lookup_04_T006A/schema.json',
            'test/resources/data/sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai/graph/Lookup_04_T006A/data.json',
            "in0"
        )
        from sap_md_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.graph.Lookup_04_T006A import Lookup_04_T006A
        Lookup_04_T006A(self.spark, dfgraph_Lookup_04_T006A)
