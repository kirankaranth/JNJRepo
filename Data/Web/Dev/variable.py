import os

schema = "WORK"
all_columns = "*"
release = "CDL L1 2022 R12"
underlying_l1_db_for_vw = ""

# Databricks credentials from env
token = os.getenv("DATABRICKS_TOKEN")
url = os.getenv("DATABRICKS_URL")
system = (os.getenv("SYSTEM_UNDER_TEST") if "SYSTEM_UNDER_TEST" in os.environ else "panda")
databricks_env = os.getenv("DATABRICKS_ENV")
databricks_user_id = os.getenv("DATABRICKS_USER_ID")

# Databricks credentials from env for views
vw_token = os.getenv("VW_DATABRICKS_TOKEN")
vw_url = os.getenv("VW_DATABRICKS_URL")
vw_system = os.getenv("VW_SYSTEM_UNDER_TEST")

if databricks_env.upper() == "DEV" or databricks_env.upper() == "PROD":
    underlying_l1_db_for_vw = "l1_db"
elif databricks_env.upper() == "QA":
    underlying_l1_db_for_vw = "qa_l1_db"

# Optional conditions
table_limit = (
    os.getenv("TABLE_ROW_LIMIT") if "TABLE_ROW_LIMIT" in os.environ else "none"
)
test_tables = (
    os.getenv("TABLES_UNDER_TEST") if "TABLES_UNDER_TEST" in os.environ else "all"
)

# Set Jira Tag of story under test
JIRA_ID = os.getenv("JIRA_ID") if "JIRA_ID" in os.environ else "XXXX"

DICT__TABLE_ALIAS = {"panda": "pda", "europe2": "eu2", "atlas": "atl", "sustain": "sus", "elims": "elm", "maximo":"max", "p01": "p01", "btb_na": "bbn", "btb_latam": "bbl","taishan": "tai","bw2": "bw2","deu": "deu","etq_instinct": "eti", "bba": "bba", "hcs": "hcs", "mrs": "mrs", "trackwise": "trw", "bwi": "bwi", "gmd": "gmd", "geu": "geu", "p360": "p36", "prd_9ec": "9ec", "prd_9pq": "9pq", "hmd": "hmd", "jet": "jet", "jsw": "jsw", "fsn": "fsn", "llj":"llj","llm":"llm","llv": "llv","llp": "llp", "svs": "svs","mle":"mle","mln":"mln","oas": "oas","mbp": "mbp", "mtr": "mtr","jes": "jes","lle": "lle","hm2": "hm2","atl":"atl","bbn":"bbn","bbl":"bbl"}
