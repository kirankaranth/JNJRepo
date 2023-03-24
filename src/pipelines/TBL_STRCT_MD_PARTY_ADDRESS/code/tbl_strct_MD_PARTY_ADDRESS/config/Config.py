from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            targetSchema: str=None,
            targetEnv: str=None,
            targetDomain: str=None,
            targetApp: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(targetSchema, targetEnv, targetDomain, targetApp)

    def update(
            self,
            targetSchema: str="l1_md_prophecy",
            targetEnv: str="dev",
            targetDomain: str="MD_PARTY_ADDRESS",
            targetApp: str="md_l1",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.targetSchema = targetSchema
        self.targetEnv = targetEnv
        self.targetDomain = targetDomain
        self.targetApp = targetApp
        pass
