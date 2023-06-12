from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_MFG_ORDER_ITM_HM2_HMD',
    version = '1.0',
    packages = find_packages(include = ('sap_md_mfg_order_itm_hm2_hmd*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_md_mfg_order_itm_hm2_hmd.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)