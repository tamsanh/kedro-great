from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="kedro-great",
    version="0.2.7",
    url="https://github.com/tamsanh/kedro-great.git",
    author="Tam-Sanh Nguyen",
    author_email="tamsanh@gmail.com",
    description="Kedro Great makes integrating Great Expectations with Kedro easy!",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=["kedro_great"],
    zip_safe=False,
    include_package_data=True,
    license="MIT",
    install_requires=[
        "kedro>=0.16.0",
        "kedro[pandas]>=0.16.0",
        "kedro[spark]>=0.16.0",
        "great_expectations",
        "pyspark",
        "pandas",
    ],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    entry_points={
        "kedro.global_commands": ["kedro-great = kedro_great:commands"]
    }
)
