""" Setuptools-based setup module for SAMADhi

derived from the pypa example, see https://github.com/pypa/sampleproject
"""

import os
import os.path
from distutils.command.build_py import build_py

from setuptools import find_packages, setup

# Get the long description from the relevant file


here = os.path.abspath(os.path.dirname(__file__))

class build_py_with_init(build_py):
    def run(self):
        if not self.dry_run:
            for pkgNm in ("cp3_llbb", os.path.join("cp3_llbb", "SAMADhi")):
                pkgDir = os.path.join(self.build_lib, pkgNm)
                self.mkpath(pkgDir)
                with open(os.path.join(pkgDir, "__init__.py"), "w") as initf:
                    initf.write("")
        build_py.run(self)

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="SAMADhi",

    version="2.1.0",

    description="SAmple MAnagement Database",
    long_description=long_description,
    long_description_content_type="text/markdown",

    url="https://github.com/cp3-llbb/SAMADhi",

    author="Christophe Delaere",
    author_email="christophe.delaere@uclouvain.be",

    license="unknown",

    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'License :: Other/Proprietary License',

        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
    keywords='database',

    install_requires=["peewee", "pymysql", "future"],
    tests_require=["pytest", "pytest-console-scripts"],

    packages=["cp3_llbb.SAMADhi"],
    package_dir={"cp3_llbb.SAMADhi": "python"},
    cmdclass={"build_py": build_py_with_init},

    entry_points={
        "console_scripts": [
            "search_SAMADhi=cp3_llbb.SAMADhi.scripts:search",
            "iSAMADhi=cp3_llbb.SAMADhi.scripts:interactive",
            "update_datasets_cross_section=cp3_llbb.SAMADhi.scripts:update_datasets_cross_section",
            "add_sample=cp3_llbb.SAMADhi.scripts:add_sample",
            "add_result=cp3_llbb.SAMADhi.scripts:add_result",
            "checkAndClean=cp3_llbb.SAMADhi.scripts:checkAndClean",
            "das_import=cp3_llbb.SAMADhi.das_import:main",
            "das_import_nanoAOD=cp3_llbb.SAMADhi.das_import:import_nanoAOD_sample",
            "compute_sample_luminosity=cp3_llbb.SAMADhi.luminosity:compute_sample_luminosity",
            "SAMADhi_dbAnalysis=cp3_llbb.SAMADhi.dbAnalysis:main"
            ]
        },
)
