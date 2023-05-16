from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.graph.SchemaTransform_1 import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.config.ConfigStore import *


class SchemaTransform_1Test(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd/graph/SchemaTransform_1/in0/schema.json',
            'test/resources/data/md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd/graph/SchemaTransform_1/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd/graph/SchemaTransform_1/out/schema.json',
            'test/resources/data/md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd/graph/SchemaTransform_1/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = SchemaTransform_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("GTIN_NUM_HRMZD"),
            dfOutComputed.select("GTIN_NUM_HRMZD"),
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
        dfgraph_MEINS_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd/graph/MEINS_LU/schema.json',
            'test/resources/data/md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd/graph/MEINS_LU/data.json',
            "in0"
        )
        from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.graph.MEINS_LU import MEINS_LU
        MEINS_LU(self.spark, dfgraph_MEINS_LU)
