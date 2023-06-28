from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_SLS_ORDR_HIST_LDGR_JES',
    version = '1.0',
    packages = find_packages(include = ('jde_md_sls_ordr_hist_ldgr_jes*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
'console_scripts' : [
'main = jde_md_sls_ordr_hist_ldgr_jes.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
