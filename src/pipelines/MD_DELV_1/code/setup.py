from setuptools import setup, find_packages
setup(
    name = 'MD_DELV_1_bba_btb_latam_btb_na_hcs_mrs',
    version = '1.0',
    packages = find_packages(include = ('MD_DELV_1*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = MD_DELV_1.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
