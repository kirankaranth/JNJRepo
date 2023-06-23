from setuptools import setup, find_packages
setup(
    name = 'sap_md_doc_itm_incm_invc_hm2_hmd_bba_bbl_bbn_geu_mrs_po1_tai',
    version = '1.0',
    packages = (
      find_packages(include = ('sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai*', ))
      + ["prophecy_config_instances"]
    ),
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
'console_scripts' : [
'main = sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
