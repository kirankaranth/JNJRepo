from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_BOM_HDR_ATL_BBL_BWI_FSN_GEU_MBP_P01_SVS_TAI',
    version = '1.0',
    packages = find_packages(include = ('sap_md_bom_hdr_atl_bbl_bwi_fsn_geu_mbp_p01_svs_tai*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.8'],
    entry_points = {
'console_scripts' : [
'main = sap_md_bom_hdr_atl_bbl_bwi_fsn_geu_mbp_p01_svs_tai.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
