from setuptools import setup, find_packages
setup(
    name = 'MD_SLS_ORDR_SCHED_LINE_DELV_5_atlas_fsn',
    version = '1.0',
    packages = find_packages(include = ('MD_SLS_ORDR_SCHED_LINE_DELV_5*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = MD_SLS_ORDR_SCHED_LINE_DELV_5.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
