from setuptools import setup, find_packages
setup(
    name = 'SAP_01_MD_CUST_MRS_FSN_SVS_GEU_MBP',
    version = '1.0',
    packages = find_packages(include = ('sap_01_md_cust_mrs_fsn_svs_geu_mbp*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_01_md_cust_mrs_fsn_svs_geu_mbp.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
