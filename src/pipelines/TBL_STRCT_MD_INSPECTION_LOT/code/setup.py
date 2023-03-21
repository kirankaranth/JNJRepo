from setuptools import setup, find_packages
setup(
    name = 'TBL_STRCT_MD_INSPECTION_LOT',
    version = '1.0',
    packages = find_packages(include = ('tbl_strct_MD_INSPECTION_LOT*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.2'],
    entry_points = {
'console_scripts' : [
'main = tbl_strct_MD_INSPECTION_LOT.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
