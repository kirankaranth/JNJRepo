from setuptools import setup, find_packages
setup(
    name = 'MD_SLS_ORDR_LINE_MBP',
    version = '1.0',
    packages = find_packages(include = ('sap_01_md_sls_ordr_line*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.8'],
    entry_points = {
'console_scripts' : [
'main = sap_01_md_sls_ordr_line.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
