from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_PRCH_DELV_CNFRMS_3_jsw_jem_jet_jes_deu_sjd_gmd_djd',
    version = '1.0',
    packages = (
      find_packages(include = ('jde_md_prch_delv_cnfrms_3_jsw_jem_jet_jes_deu_sjd_gmd_djd*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_md_prch_delv_cnfrms_3_jsw_jem_jet_jes_deu_sjd_gmd_djd.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
