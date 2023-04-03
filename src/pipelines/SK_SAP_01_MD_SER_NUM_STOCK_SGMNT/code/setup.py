from setuptools import setup, find_packages
setup(
    name = 'SK_SAP_01_MD_SER_NUM_STOCK_SGMNT',
    version = '1.0',
    packages = find_packages(include = ('sk_sap_01_md_ser_num_stock_sgmnt*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.2'],
    entry_points = {
'console_scripts' : [
'main = sk_sap_01_md_ser_num_stock_sgmnt.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
