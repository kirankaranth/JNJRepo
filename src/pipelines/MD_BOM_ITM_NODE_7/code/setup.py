from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_BOM_ITM_NODE_TAI_BWI_MBP_HCS_MRS_P01_GEU_BBN_BBL_BBA_ATL_FSN_SVS',
    version = '1.0',
    packages = (
      find_packages(include = ('sap_md_bom_itm_node_tai_bwi_mbp_hcs_mrs_p01_geu_bbn_bbl_bba_atl_fsn_svs*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
      'console_scripts': ['main = sap_md_bom_itm_node_tai_bwi_mbp_hcs_mrs_p01_geu_bbn_bbl_bba_atl_fsn_svs.pipeline:main'],
    },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
