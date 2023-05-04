from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_BOM_HDR_DEU_SJD_MTR_GMD_JEM_JSW_BW2_JES_JET',
    version = '1.0',
    packages = find_packages(include = ('jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
