from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_DELV_LINE_BW2_GMD_JET_JSW_DEU_MTR_SJD_DJD_JEM_JES',
    version = '1.0',
    packages = (
      find_packages(include = ('jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
