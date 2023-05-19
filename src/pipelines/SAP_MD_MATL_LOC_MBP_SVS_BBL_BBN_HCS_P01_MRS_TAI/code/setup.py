from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MATL_LOC_MBP_SVS_BBL_BBN_HCS_P01_MRS_TAI',
    version = '1.0',
    packages = find_packages(include = ('sap_md_matl_loc_mbp_svs_bbn_p01_mrs*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_md_matl_loc_mbp_svs_bbn_p01_mrs.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
