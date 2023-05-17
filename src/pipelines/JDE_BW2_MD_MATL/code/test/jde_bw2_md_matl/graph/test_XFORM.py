from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_bw2_md_matl.graph.XFORM import *
from jde_bw2_md_matl.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_bw2_md_matl/graph/XFORM/in0/schema.json',
            'test/resources/data/jde_bw2_md_matl/graph/XFORM/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_bw2_md_matl/graph/XFORM/out/schema.json',
            'test/resources/data/jde_bw2_md_matl/graph/XFORM/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CHG_BY"), dfOutComputed.select("CHG_BY"), self.maxUnequalRowsToShow)

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
        dfgraph_FRAN_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_bw2_md_matl/graph/FRAN_LU/schema.json',
            'test/resources/data/jde_bw2_md_matl/graph/FRAN_LU/data.json',
            "in0"
        )
        from jde_bw2_md_matl.graph.FRAN_LU import FRAN_LU
        FRAN_LU(self.spark, dfgraph_FRAN_LU)
        dfgraph_SLD_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_bw2_md_matl/graph/SLD_LU/schema.json',
            'test/resources/data/jde_bw2_md_matl/graph/SLD_LU/data.json',
            "in0"
        )
        from jde_bw2_md_matl.graph.SLD_LU import SLD_LU
        SLD_LU(self.spark, dfgraph_SLD_LU)
        dfgraph_BRAVO_D_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_bw2_md_matl/graph/BRAVO_D_LU/schema.json',
            'test/resources/data/jde_bw2_md_matl/graph/BRAVO_D_LU/data.json',
            "in0"
        )
        from jde_bw2_md_matl.graph.BRAVO_D_LU import BRAVO_D_LU
        BRAVO_D_LU(self.spark, dfgraph_BRAVO_D_LU)
        dfgraph_MATL_T_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_bw2_md_matl/graph/MATL_T_LU/schema.json',
            'test/resources/data/jde_bw2_md_matl/graph/MATL_T_LU/data.json',
            "in0"
        )
        from jde_bw2_md_matl.graph.MATL_T_LU import MATL_T_LU
        MATL_T_LU(self.spark, dfgraph_MATL_T_LU)
        dfgraph_MATL_GR_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_bw2_md_matl/graph/MATL_GR_LU/schema.json',
            'test/resources/data/jde_bw2_md_matl/graph/MATL_GR_LU/data.json',
            "in0"
        )
        from jde_bw2_md_matl.graph.MATL_GR_LU import MATL_GR_LU
        MATL_GR_LU(self.spark, dfgraph_MATL_GR_LU)
