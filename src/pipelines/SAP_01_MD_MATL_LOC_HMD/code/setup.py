from setuptools import setup, find_packages
setup(
    name = 'SAP_01_MD_MATL_LOC_HMD',
    version = '1.0',
    packages = find_packages(include = ('sap_01_md_matl_loc_hmd*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_01_md_matl_loc_hmd.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
