from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_SLS_ORDR_SCHED_LINE_DELV_bwi',
    version = '1.0',
    packages = find_packages(include = ('md_sls_ordr_sched_line_delv_bwi*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = md_sls_ordr_sched_line_delv_bwi.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
