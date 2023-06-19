from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_MATL_MVMT_HDR_GMD_v2',
    version = '1.0',
    packages = find_packages(include = ('jde_md_matl_mvmt_hdr_gmd_v2*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.4'],
    entry_points = {
'console_scripts' : [
'main = jde_md_matl_mvmt_hdr_gmd_v2.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
