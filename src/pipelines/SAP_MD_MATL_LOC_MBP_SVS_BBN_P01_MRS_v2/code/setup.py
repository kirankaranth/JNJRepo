from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MATL_LOC_MBP_SVS_BBN_P01_MRS_v2',
    version = '1.0',
    packages = find_packages(include = ('sap_md_matl_loc_mbp_svs_bbn_p01_mrs_v2*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
'console_scripts' : [
'main = sap_md_matl_loc_mbp_svs_bbn_p01_mrs_v2.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
