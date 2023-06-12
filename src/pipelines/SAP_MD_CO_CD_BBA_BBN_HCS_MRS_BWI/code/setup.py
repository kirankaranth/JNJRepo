from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_CO_CD_BBA_BBN_HCS_MRS_BWI',
    version = '1.0',
    packages = find_packages(include = ('sap_md_co_cd_bba_bbn_hcs_mrs_bwi*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_md_co_cd_bba_bbn_hcs_mrs_bwi.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
