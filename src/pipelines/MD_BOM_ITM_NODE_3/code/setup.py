from setuptools import setup, find_packages
setup(
    name = 'MD_BOM_ITM_NODE_3_deu_jem',
    version = '1.0',
    packages = find_packages(include = ('MD_BOM_ITM_NODE_3*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = MD_BOM_ITM_NODE_3.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
