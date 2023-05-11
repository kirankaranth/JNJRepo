from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MATL_BOM_ATL_BBA_BBL_BBN_HCS_MBP_MRS_P01_SVS_BWI_FSN_TAI_GEU_HMD',
    version = '1.0',
    packages = (
      find_packages(include = ('sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
      'console_scripts': ['main = sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.pipeline:main'],
    },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
