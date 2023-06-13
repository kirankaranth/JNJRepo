from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_SLS_ORDR_SCHED_LINE_DELV_mbp',
    version = '1.0',
    packages = find_packages(include = ('sap_md_sls_ordr_sched_line_delv_mbp*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
'console_scripts' : [
'main = sap_md_sls_ordr_sched_line_delv_mbp.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
