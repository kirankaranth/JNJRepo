from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MATL_ATL_BWI_FSN_GEU_MRS_P01_SVS_TAI',
    version = '1.0',
    packages = find_packages(include = ('md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = md_matl_sap_atl_bwi_fsn_geu_mrs_p01_svs_tai.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
