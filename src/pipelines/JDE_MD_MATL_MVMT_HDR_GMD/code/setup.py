from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_MATL_MVMT_HDR_GMD',
    version = '1.0',
    packages = find_packages(include = ('jde_md_matl_mvmt_hdr_gmd*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_md_matl_mvmt_hdr_gmd.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
