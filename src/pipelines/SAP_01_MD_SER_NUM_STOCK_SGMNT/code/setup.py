from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_SER_NUM_STOCK_SGMNT_ATL_BBA_BBL_BBN_MRS',
    version = '1.0',
    packages = find_packages(include = ('sap_01_md_ser_num_stock_sgmnt*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.7'],
    entry_points = {
'console_scripts' : [
'main = sap_01_md_ser_num_stock_sgmnt.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
