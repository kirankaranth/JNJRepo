from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_DELV_BW2_GMD_JET_JSW_MTR_DEU_JES_JEM_SJD_DJD',
    version = '1.0',
    packages = find_packages(include = ('sap_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_md_delv_bw2_gmd_jet_jsw_mtr_deu_jes_jem_sjd_djd.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
