from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_CRNCY_TEXT_GMD_BW2_DEU_JES_JEM_JET_JSW_SJD_DJD',
    version = '1.0',
    packages = find_packages(include = ('jde_md_crncy_text_gmd_bw2_deu_jes_jem_jet_jsw_sjd_djd*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_md_crncy_text_gmd_bw2_deu_jes_jem_jet_jsw_sjd_djd.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
