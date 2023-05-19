from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            sourceSystem: str=None,
            targetSchema: str=None,
            sourceDatabase: str=None,
            DAI_ETL_ID: int=None,
            ConfigDatabase: str=None,
            sourceTable: str=None,
            CRT_DTTM: str=None,
            AVAIL_DTTM: str=None,
            CHG_DTTM: str=None,
            BTCH_EXP_DTTM: str=None,
            SUI_IND: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            sourceSystem, 
            targetSchema, 
            sourceDatabase, 
            DAI_ETL_ID, 
            ConfigDatabase, 
            sourceTable, 
            CRT_DTTM, 
            AVAIL_DTTM, 
            CHG_DTTM, 
            BTCH_EXP_DTTM, 
            SUI_IND
        )

    def update(
            self,
            sourceSystem: str="jem",
            targetSchema: str="dev_md_l1",
            sourceDatabase: str="jem",
            DAI_ETL_ID: int=0,
            ConfigDatabase: str=" ",
            sourceTable: str="f4108",
            CRT_DTTM: str="CASE WHEN LOWER(TRIM(ioohdj)) IS NULL OR TRIM(ioohdj) = '' OR TRIM(ioohdj) = '0' THEN NULL ELSE to_timestamp(date_add(cast(substring(ioohdj+ 1900000,1,4) AS Date),cast(substring(ioohdj,4,3) as int)-1)) end",
            AVAIL_DTTM: str="CASE WHEN LOWER(TRIM(iodlej)) IS NULL OR TRIM(iodlej) = '' OR TRIM(iodlej) = '0' THEN NULL ELSE to_timestamp(date_add(cast(substring(iodlej+ 1900000,1,4) AS Date),cast(substring(iodlej,4,3) as int)-1)) end",
            CHG_DTTM: str="CASE WHEN LOWER(TRIM(ioupmj)) IS NULL OR TRIM(ioupmj) = '' OR TRIM(ioupmj) = '0' THEN NULL ELSE to_timestamp(date_add(cast(substring(ioupmj+ 1900000,1,4) AS Date),cast(substring(ioupmj,4,3) as int)-1)) end",
            BTCH_EXP_DTTM: str="CASE WHEN LOWER(TRIM(iommej)) IS NULL OR TRIM(iommej) = '' OR TRIM(iommej) = '0' THEN NULL ELSE to_timestamp(date_add(cast(substring(iommej+ 1900000,1,4) AS Date),cast(substring(iommej,4,3) as int)-1)) end",
            SUI_IND: str="cast(null as string)",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.sourceSystem = sourceSystem
        self.targetSchema = targetSchema
        self.sourceDatabase = sourceDatabase
        self.DAI_ETL_ID = self.get_int_value(DAI_ETL_ID)
        self.ConfigDatabase = ConfigDatabase
        self.sourceTable = sourceTable
        self.CRT_DTTM = CRT_DTTM
        self.AVAIL_DTTM = AVAIL_DTTM
        self.CHG_DTTM = CHG_DTTM
        self.BTCH_EXP_DTTM = BTCH_EXP_DTTM
        self.SUI_IND = SUI_IND
        pass
