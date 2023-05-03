from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            targetSchema: str=None,
            sourceDatabase: str=None,
            sourceSystem: str=None,
            configDatabase: str=None,
            DBTABLE: str=None,
            DBTABLE1: str=None,
            DAI_ETL_ID: int=None,
            BRAVO_MINOR_DESC_FILTER: str=None,
            B_M_LU_Field: str=None,
            FRAN_CD_FILTER: str=None,
            F_C_LU_Field: str=None,
            MATL_GRP_FILTER: str=None,
            M_G_LU_Field: str=None,
            MATL_TYPE_DESC_FILTER: str=None,
            M_T_D_LU_Field: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            targetSchema, 
            sourceDatabase, 
            sourceSystem, 
            configDatabase, 
            DBTABLE, 
            DBTABLE1, 
            DAI_ETL_ID, 
            BRAVO_MINOR_DESC_FILTER, 
            B_M_LU_Field, 
            FRAN_CD_FILTER, 
            F_C_LU_Field, 
            MATL_GRP_FILTER, 
            M_G_LU_Field, 
            MATL_TYPE_DESC_FILTER, 
            M_T_D_LU_Field
        )

    def update(
            self,
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="gmd",
            sourceSystem: str="gmd",
            configDatabase: str=" ",
            DBTABLE: str="f4101",
            DBTABLE1: str="f0005",
            DAI_ETL_ID: int=0,
            BRAVO_MINOR_DESC_FILTER: str="Filter N/A",
            B_M_LU_Field: str="Field N/A",
            FRAN_CD_FILTER: str="Filter N/A",
            F_C_LU_Field: str="Field N/A",
            MATL_GRP_FILTER: str="Filter N/A",
            M_G_LU_Field: str="Field N/A",
            MATL_TYPE_DESC_FILTER: str="Filter N/A",
            M_T_D_LU_Field: str="Field N/A",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.sourceSystem = sourceSystem
        self.configDatabase = configDatabase
        self.DBTABLE = DBTABLE
        self.DBTABLE1 = DBTABLE1
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.BRAVO_MINOR_DESC_FILTER = BRAVO_MINOR_DESC_FILTER
        self.B_M_LU_Field = B_M_LU_Field
        self.FRAN_CD_FILTER = FRAN_CD_FILTER
        self.F_C_LU_Field = F_C_LU_Field
        self.MATL_GRP_FILTER = MATL_GRP_FILTER
        self.M_G_LU_Field = M_G_LU_Field
        self.MATL_TYPE_DESC_FILTER = MATL_TYPE_DESC_FILTER
        self.M_T_D_LU_Field = M_T_D_LU_Field
        pass
