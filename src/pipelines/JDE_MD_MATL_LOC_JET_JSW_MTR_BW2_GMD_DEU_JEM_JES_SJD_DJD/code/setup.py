from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_MATL_LOC_JET_JSW_MTR_BW2_GMD_DEU_JEM_JES_SJD_DJD',
    version = '1.0',
    packages = find_packages(include = ('jde_md_matl_loc_deu*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_md_matl_loc_deu.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
