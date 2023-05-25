from setuptools import setup, find_packages
setup(
    name = 'MD_CRNCY_TEXT_2_gmd_bw2_deu_jem_jes_jet_jsw_sjd',
    version = '1.0',
    packages = find_packages(include = ('MD_CRNCY_TEXT_2*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = MD_CRNCY_TEXT_2.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
