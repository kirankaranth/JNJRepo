from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            MANDT: str=None,
            COLUMNS: int=None,
            DBNAME: str=None,
            DAI_ETL_ID: int=None,
            SHIP_TO: str=None,
            PSTNG_BILL_STS_CD: str=None,
            INVC_LIST_STS_CD: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            targetSchema, 
            MANDT, 
            COLUMNS, 
            DBNAME, 
            DAI_ETL_ID, 
            SHIP_TO, 
            PSTNG_BILL_STS_CD, 
            INVC_LIST_STS_CD
        )

    def update(
            self,
            sourceSystem: str="bba",
            targetSchema: str="l1_md_prophecy",
            MANDT: str="100",
            COLUMNS: int=80,
            DBNAME: str="bba",
            DAI_ETL_ID: int=0,
            SHIP_TO: str="CAST(NULL\u00A0AS\u00A0STRING)",
            PSTNG_BILL_STS_CD: str="CAST(NULL\u00A0AS\u00A0STRING)",
            INVC_LIST_STS_CD: str="CAST(NULL\u00A0AS\u00A0STRING)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.MANDT = MANDT
        self.COLUMNS = self.get_int_value(COLUMNS)
        self.DBNAME = DBNAME
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.SHIP_TO = SHIP_TO
        self.PSTNG_BILL_STS_CD = PSTNG_BILL_STS_CD
        self.INVC_LIST_STS_CD = INVC_LIST_STS_CD
        pass
