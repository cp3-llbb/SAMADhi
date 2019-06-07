from __future__ import unicode_literals
""" Setuptools-based setup module for SAMADhi

derived from the pypa example, see https://github.com/pypa/sampleproject
"""

from setuptools import setup, find_packages
import os, os.path

here = os.path.abspath(os.path.dirname(__file__))

from distutils.command.build_py import build_py
class build_py_with_init(build_py):
    def run(self):
        if not self.dry_run:
            for pkgNm in ("cp3_llbb", os.path.join("cp3_llbb", "SAMADhi")):
                pkgDir = os.path.join(self.build_lib, pkgNm)
                self.mkpath(pkgDir)
                with open(os.path.join(pkgDir, "__init__.py"), "w") as initf:
                    initf.write("")
        build_py.run(self)

# Get the long description from the relevant file
from io import open
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

    install_requires=["peewee", "pymysql"],
    tests_require=["pytest", "pytest-console-scripts"],

    packages=["cp3_llbb.SAMADhi"],
    package_dir={"cp3_llbb.SAMADhi": "python"},
    cmdclass={"build_py": build_py_with_init},

    scripts=[ os.path.join(root, item) for root, subFolder, files in os.walk("scripts") for item in files ],
)
