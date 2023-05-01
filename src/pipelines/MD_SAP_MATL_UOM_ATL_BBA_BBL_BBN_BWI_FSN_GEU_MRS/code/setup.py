from setuptools import setup, find_packages
setup(
    name = 'MD_SAP_MATL_UOM_ATL_BBA_BBL_BBN_BWI_FSN_GEU_MRS_TAI',
    version = '1.0',
    packages = find_packages(include = ('md_sap_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = md_sap_matl_uom_atl_bba_bbl_bbn_bwi_fsn_geu_mrs_tai.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
