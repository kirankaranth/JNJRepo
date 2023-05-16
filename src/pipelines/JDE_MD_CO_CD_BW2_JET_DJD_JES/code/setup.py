from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_CO_CD_BW2_JET_DJD_JES',
    version = '1.0',
    packages = find_packages(include = ('jde_md_co_cd_bw2_jet_djd_jes*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_md_co_cd_bw2_jet_djd_jes.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
