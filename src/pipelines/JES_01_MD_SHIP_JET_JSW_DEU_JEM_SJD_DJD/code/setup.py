from setuptools import setup, find_packages
setup(
    name = 'JES_01_MD_SHIP_JET_JSW_DEU_JEM_SJD_DJD',
    version = '1.0',
    packages = find_packages(include = ('jes_01_md_ship_jet_jsw_deu_jem_sjd_djd*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.8'],
    entry_points = {
'console_scripts' : [
'main = jes_01_md_ship_jet_jsw_deu_jem_sjd_djd.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
