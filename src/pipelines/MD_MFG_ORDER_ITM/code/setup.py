from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MFG_ORDER_ITM_MBP_BWI_BBN_BBL_BBA_MRS_P01_TAI',
    version = '1.0',
    packages = find_packages(include = ('sap_md_mfg_order_itm_mbp_bwi_bbn_bbl_bba_mrs_p01_tai*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_md_mfg_order_itm_mbp_bwi_bbn_bbl_bba_mrs_p01_tai.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
