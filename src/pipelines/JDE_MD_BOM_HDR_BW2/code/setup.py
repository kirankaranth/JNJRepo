from setuptools import setup, find_packages
setup(
    name = 'JDE_MD_BOM_HDR_BW2',
    version = '1.0',
    packages = find_packages(include = ('jde_md_bom_hdr_bw2*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.4.7'],
    entry_points = {
'console_scripts' : [
'main = jde_md_bom_hdr_bw2.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
