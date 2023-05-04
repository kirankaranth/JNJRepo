from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_SLS_ORDR_SCHED_LINE_DELV_gmd_bw2_jet_jem_jes_jsw_djd_sjd_mtr_deu',
    version = '1.0',
    packages = (
      find_packages(include = ('jde_md_sls_ordr_sched_line_delv_gmd_bw2_jet_jem_jes_jsw_djd_sjd_mtr_deu*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
      'console_scripts': ['main = jde_md_sls_ordr_sched_line_delv_gmd_bw2_jet_jem_jes_jsw_djd_sjd_mtr_deu.pipeline:main'],
    },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
