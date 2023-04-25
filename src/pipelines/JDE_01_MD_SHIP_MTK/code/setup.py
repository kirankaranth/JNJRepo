from setuptools import setup, find_packages
setup(
    name = 'JDE_01_MD_SHIP_MTR_BW2_GMD_JES',
    version = '1.0',
    packages = find_packages(include = ('jde_01_md_ship_mtr_bw2_gmd_jes*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_01_md_ship_mtr_bw2_gmd_jes.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
