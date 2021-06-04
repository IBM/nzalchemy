import os
import re

from setuptools import setup, find_packages

v = open(
    os.path.join(os.path.dirname(__file__), "nzalchemy", "__init__.py")
)
VERSION = re.compile(r'.*__version__ = "(.*?)"', re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), '..',"README.md")


setup(
    name="nzalchemy",
    version=VERSION,
    description="Netezza Dialect for SQLAlchemy",
    long_description=open(readme).read(),
    long_description_content_type='text/markdown',
    url="https://github.com/IBM/nzalchemy",
    author="",
    author_email="",
    license="IBM",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
    ],
    keywords="SQLAlchemy IBM nz",
    project_urls={
        "Documentation": "https://github.com/IBM/nzalchemy/wiki",
        "Source": "https://github.com/IBM/nzalchemy",
        "Tracker": "https://github.com/IBM/nzalchemy/issues",
    },
    packages=find_packages(include=["nzalchemy"]),
    include_package_data=True,
    install_requires=["SQLAlchemy<=1.3.24"],
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": [
            "netezza.pyodbc = nzalchemy.pyodbc:NetezzaDialect_pyodbc",
            "netezza.nzpy = nzalchemy.nzpy:NetezzaDialect_nzpy"
        ]
    },
)
