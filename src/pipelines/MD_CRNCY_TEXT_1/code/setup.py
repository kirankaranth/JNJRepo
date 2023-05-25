from setuptools import setup, find_packages
setup(
    name = 'MD_CRNCY_TEXT_BBL_BBN_MRS_P01_MBP_BWI_SVS_ATL',
    version = '1.0',
    packages = find_packages(include = ('md_crncy_text_bbl_bbn_mrs_p01_mbp_bwi_svs_atl*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = md_crncy_text_bbl_bbn_mrs_p01_mbp_bwi_svs_atl.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
