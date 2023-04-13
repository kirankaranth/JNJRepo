from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_BOM_HDR_BBA_BBN_HCS_MRS',
    version = '1.0',
    packages = find_packages(include = ('sap_md_bom_hdr_bba_bbn_hcs_mrs*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.8'],
    entry_points = {
'console_scripts' : [
'main = sap_md_bom_hdr_bba_bbn_hcs_mrs.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
