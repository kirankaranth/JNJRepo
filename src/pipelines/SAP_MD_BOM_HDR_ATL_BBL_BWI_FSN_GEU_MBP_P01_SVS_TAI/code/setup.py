from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_BOM_HDR_ATL_BBA_BBL_BBN_BWI_FSN_GEU_HCS_MBP_MRS_P01_SVS_TAI_HMD',
    version = '1.0',
    packages = (
      find_packages(include = ('sap_md_bom_hdr_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
      'console_scripts': ['main = sap_md_bom_hdr_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.pipeline:main'],
    },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
