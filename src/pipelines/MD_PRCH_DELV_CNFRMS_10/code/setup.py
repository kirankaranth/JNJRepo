from setuptools import setup, find_packages
setup(
    name = 'MD_PRCH_DELV_CNFRMS_10_bwi',
    version = '1.0',
    packages = find_packages(include = ('md_prch_delv_cnfrms_10_bwi*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = md_prch_delv_cnfrms_10_bwi.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
