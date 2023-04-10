from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_MATL_INV_BW2_DEU_DJD_GMD_JEM_JES_JET_JSW_MTR_SDJ',
    version = '1.0',
    packages = find_packages(include = ('jde_01_md_matl_inv*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.7'],
    entry_points = {
'console_scripts' : [
'main = jde_01_md_matl_inv.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
