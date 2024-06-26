from setuptools import setup, find_packages
setup(
    name = 'JDE_01_MD_PO_SCHED_LINE_DELV_JES_JEM_JET_JSW_MTR_SJD_DEU_GMD_BW2',
    version = '1.0',
    packages = find_packages(include = ('jde_01_md_po_sched_line_delv*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.5.0'],
    entry_points = {
'console_scripts' : [
'main = jde_01_md_po_sched_line_delv.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
