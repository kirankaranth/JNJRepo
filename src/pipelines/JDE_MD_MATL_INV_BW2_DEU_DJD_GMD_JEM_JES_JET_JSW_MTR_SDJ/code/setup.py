from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_MATL_ALT_UOM_BW2_DEU_DJD_GMD_JEM_JES_JET_JSW_MTR_SDJ',
    version = '1.0',
    packages = (
      find_packages(include = ('jde_md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.8'],
    entry_points = {
'console_scripts' : [
'main = jde_md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
