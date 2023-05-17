from setuptools import setup, find_packages
setup(
    name = 'SAP_01_MD_BILL_DOC_HDR_FSN_GEU_HCS_MBP_MRS_P01_SVS_TAI',
    version = '1.0',
    packages = find_packages(include = ('sap_01_md_bill_doc_hdr_fsn_geu_hcs_mbp_mrs_p01_svs_tai*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = sap_01_md_bill_doc_hdr_fsn_geu_hcs_mbp_mrs_p01_svs_tai.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
