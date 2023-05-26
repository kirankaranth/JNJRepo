from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_SUP_CO_DEU_GMD_JSW_MTR_SJD_JEM',
    version = '1.0',
    packages = find_packages(include = ('jde_md_sup_co_deu_gmd_jsw_mtr_sjd_jem*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_md_sup_co_deu_gmd_jsw_mtr_sjd_jem.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
