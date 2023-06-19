from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_CUST_MSTR_UNLD_DATA_P01_BBA_BBN_BBL_HCS_MBP_FSN',
    version = '1.0',
    packages = find_packages(include = ('sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
'console_scripts' : [
'main = sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
