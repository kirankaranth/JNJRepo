from setuptools import setup, find_packages
setup(
    name = 'SAP_MD_SLS_DOC_HIER_HMD',
    version = '1.0',
    packages = find_packages(include = ('sap_md_sls_doc_hier_hmd*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
'console_scripts' : [
'main = sap_md_sls_doc_hier_hmd.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
