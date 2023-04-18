from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MATL_INV_TAI',
    version = '1.0',
    packages = find_packages(include = ('sap_md_matl_inv_tai*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.8'],
    entry_points = {
'console_scripts' : [
'main = sap_md_matl_inv_tai.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
