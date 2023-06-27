from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.graph.NEW_FIELDS import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_pk_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/in0/data/test_pk_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/out/data/test_pk_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("SRC_SYS_CD", "CO_CD", "SUBSQ_DOC_NUM", "SUBSQ_DOC_LINE_NBR", "SUBSQ_DOC_CAT_CD"),
            dfOutComputed.select("SRC_SYS_CD", "CO_CD", "SUBSQ_DOC_NUM", "SUBSQ_DOC_LINE_NBR", "SUBSQ_DOC_CAT_CD"),
            self.maxUnequalRowsToShow
        )

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("BASE_UOM_CD", "CRNCY_CD", "MATL_NUM", "WT_UOM_CD", "VOL_UOM_CD", "SLS_UOM_CD"),
            dfOutComputed.select("BASE_UOM_CD", "CRNCY_CD", "MATL_NUM", "WT_UOM_CD", "VOL_UOM_CD", "SLS_UOM_CD"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/in0/data/test_timestamp.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/out/data/test_timestamp.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CHG_DTTM"), dfOutComputed.select("CHG_DTTM"), self.maxUnequalRowsToShow)

    def test_decimal(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/in0/data/test_decimal.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes/graph/NEW_FIELDS/out/data/test_decimal.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(dfOut.select("NET_WT_MEAS"), dfOutComputed.select("NET_WT_MEAS"), self.maxUnequalRowsToShow)

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
