from setuptools import setup, find_packages
setup(
    name = 'PPLN_MD_MATL_9_hcs',
    version = '1.0',
    packages = find_packages(include = ('PPLN_MD_MATL_9*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.2'],
    entry_points = {
'console_scripts' : [
'main = PPLN_MD_MATL_9.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
