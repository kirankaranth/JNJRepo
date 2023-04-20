from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_PO_HIST_BBA_BBL_BBN_MBP_MRS_TAI_P01_HMD_GEU_HCS',
    version = '1.0',
    packages = find_packages(include = ('dart_l1_purchase_order_md_po_hist*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = dart_l1_purchase_order_md_po_hist.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
