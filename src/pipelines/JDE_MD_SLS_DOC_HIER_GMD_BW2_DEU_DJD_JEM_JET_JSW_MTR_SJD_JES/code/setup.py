from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_SLS_DOC_HIER_GMD_BW2_DEU_DJD_JEM_JET_JSW_MTR_SJD_JES',
    version = '1.0',
    packages = (
      find_packages(include = ('jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
'console_scripts' : [
'main = jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
