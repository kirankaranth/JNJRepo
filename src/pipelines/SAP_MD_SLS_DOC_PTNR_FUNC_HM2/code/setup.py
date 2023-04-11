from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_SLS_DOC_PTNR_FUNC_HM2',
    version = '1.0',
    packages = find_packages(include = ('sap_md_sls_doc_ptnr_func_hm2*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.8'],
    entry_points = {
'console_scripts' : [
'main = sap_md_sls_doc_ptnr_func_hm2.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
