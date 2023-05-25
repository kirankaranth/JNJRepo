from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MATL_VALUT_HIST_BBN_BBA_BBL_P01_TAI_MRS_HCS_GEU_BWI_MBP_FSN_ATL_SVS_HM2_HMD',
    version = '1.0',
    packages = (
      find_packages(include = ('sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
      'console_scripts': ['main = sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd.pipeline:main'],
    },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
