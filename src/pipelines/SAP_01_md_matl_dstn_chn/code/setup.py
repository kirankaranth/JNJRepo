from setuptools import setup, find_packages
setup(
    name = 'SAP_md_matl_dstn_chn_bba_bbl_bbn_geu_hcs_mbp_mrs_p01_tai_svs_atl_fsn',
    version = '1.0',
    packages = (
      find_packages(include = ('sap_md_matl_dstn_chn_bba_bbl_bbn_geu_hcs_mbp_mrs_p01_tai_svs_atl_fsn*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
      'console_scripts': ['main = sap_md_matl_dstn_chn_bba_bbl_bbn_geu_hcs_mbp_mrs_p01_tai_svs_atl_fsn.pipeline:main'],
    },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
