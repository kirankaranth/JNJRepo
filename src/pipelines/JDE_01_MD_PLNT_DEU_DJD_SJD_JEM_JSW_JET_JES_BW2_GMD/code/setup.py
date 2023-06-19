from setuptools import setup, find_packages
setup(
    name = 'JDE_01_MD_PLNT_DEU_DJD_SJD_JEM_JSW_JET_JES_BW2_GMD',
    version = '1.0',
    packages = find_packages(include = ('jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
