from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="de1",
    version="0.1.1",
    url="https://github.com/dataengineerone/de1-python",
    author="DataEngineerOne",
    author_email="dataengineerone@gmail.com",
    description="DE1's curated collection of kedro tools.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    zip_safe=True,
    include_package_data=True,
    license="MIT",
    install_requires=[
        "kedro>=0.16.0",
    ],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    entry_points={
    }
)
