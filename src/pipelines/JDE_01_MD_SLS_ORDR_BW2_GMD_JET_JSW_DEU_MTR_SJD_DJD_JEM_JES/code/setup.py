from setuptools import setup, find_packages
setup(
    name = 'JDE_01_MD_SLS_ORDR_BW2_GMD_JET_JSW_DEU_MTR_SJD_DJD_JEM_JES',
    version = '1.0',
    packages = (
      find_packages(include = ('jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
'console_scripts' : [
'main = jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
