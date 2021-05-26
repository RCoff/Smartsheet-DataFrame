import setuptools

setuptools.setup(
    name="smartsheet-dataframe",
    version="0.1.8",
    install_requires=[
        'pandas>=1.1.0',
        'requests>=2'
    ],
    author="Ridge Coffman",
    author_email="coffman.ridge@gmail.com",
    license="MIT",
    description="Converts Smartsheet sheets and reports to a Pandas DataFrame",
    long_description="",
    long_description_content_type="text/markdown",
    url="https://github.com/RCoff/Smartsheet-DataFrame",
    keywords=['spreadsheet', 'smartsheet', 'pandas', 'dataframe'],
    classifiers=[
        "Natural Language :: English",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial :: Spreadsheet",
    ],
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6.5"
)
