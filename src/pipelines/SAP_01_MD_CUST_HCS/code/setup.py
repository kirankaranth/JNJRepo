from setuptools import setup, find_packages
setup(
    name = 'SAP_01_MD_CUST_HCS_ATL_BBN_BBL_BBA_BWI_P01_TAI',
    version = '1.0',
    packages = find_packages(include = ('sap_01_md_cust_hcs_atl_bbn_bbl_bba_bwi_p01_tai*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_01_md_cust_hcs_atl_bbn_bbl_bba_bwi_p01_tai.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
