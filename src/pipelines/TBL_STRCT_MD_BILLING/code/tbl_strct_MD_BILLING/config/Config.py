from prophecy.config import ConfigBase
prophecy_spark_context = None


class Config(ConfigBase):

    def __init__(self, targetSchema: str=None, targetEnv: str=None, targetDomain: str=None, targetApp: str=None):
        self.spark = None
        self.update(targetSchema, targetEnv, targetDomain, targetApp)

    def update(
            self,
            targetSchema: str="l1_md_prophecy", 
            targetEnv: str="dev", 
            targetDomain: str="MD_BILLING", 
            targetApp: str="md_l1"
    ):
        global prophecy_spark_context
        prophecy_spark_context = self.spark
        self.targetSchema = targetSchema
        self.targetEnv = targetEnv
        self.targetDomain = targetDomain
        self.targetApp = targetApp
        pass
