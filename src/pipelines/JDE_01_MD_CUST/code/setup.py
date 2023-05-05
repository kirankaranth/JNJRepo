from setuptools import setup, find_packages
setup(
    name = 'JDE_01_MD_CUST_GMD_MTR_BW2_JES_JSW_JET_SJD_DJD_JEM_DEU',
    version = '1.0',
    packages = find_packages(include = ('jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
