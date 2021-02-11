#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-insightly",
    version="1.0.0",
    description="Singer.io tap for extracting data from the Insightly API",
    author="Sam Woolerton",
    url="https://samwoolerton.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_insightly"],
    install_requires=["singer-python==5.9.0", "aiohttp==3.7.3"],
    extras_require={
        "dev": [
            "pylint",
            "ipdb",
            "nose",
        ]
    },
    entry_points="""
          [console_scripts]
          tap-insightly=tap_insightly:main
      """,
    packages=["tap_insightly"],
    package_data={"tap_insightly": ["tap_insightly/schemas/*.json"]},
    include_package_data=True,
)
