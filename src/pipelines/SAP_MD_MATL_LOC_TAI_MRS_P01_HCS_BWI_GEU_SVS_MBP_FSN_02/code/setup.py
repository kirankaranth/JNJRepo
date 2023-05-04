from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MATL_LOC_TAI_MRS_P01_HCS_BWI_GEU_SVS_MBP_FSN',
    version = '1.0',
    packages = find_packages(include = ('sap_md_matl_loc_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_md_matl_loc_tai_mrs_p01_hcs_bwi_geu_svs_mbp_fsn.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
