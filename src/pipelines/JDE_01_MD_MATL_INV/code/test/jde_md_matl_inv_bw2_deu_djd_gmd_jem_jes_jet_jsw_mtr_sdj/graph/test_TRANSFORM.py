from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.graph.TRANSFORM import *
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *


class TRANSFORMTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj/graph/TRANSFORM/in0/schema.json',
            'test/resources/data/jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj/graph/TRANSFORM/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj/graph/TRANSFORM/out/schema.json',
            'test/resources/data/jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj/graph/TRANSFORM/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = TRANSFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("RSTRCTD_STCK", "UNRSTRCTD_STCK"),
            dfOutComputed.select("RSTRCTD_STCK", "UNRSTRCTD_STCK"),
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
