from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MATL_INV_BBA_BBL_BBN_GEU_MRS_MBP',
    version = '1.0',
    packages = find_packages(include = ('sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.8'],
    entry_points = {
'console_scripts' : [
'main = sap_md_matl_inv_bba_bbl_bbn_geu_mrs_mbp.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
