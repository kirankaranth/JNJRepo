from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_DELV_LINE_BWI_TAI',
    version = '1.0',
    packages = find_packages(include = ('sap_md_delv_line_bwi_tai*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_md_delv_line_bwi_tai.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
