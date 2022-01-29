import setuptools

with open("README.md", "r", encoding="utf-8") as readme_file:
    long_description = readme_file.read()

setuptools.setup(
    name="aio",
    version="0.0.1",
    description="The purpose of this package is to provide asynchronous variants of the builtin `input` and `print` functions.",
    packages=["aio"],
    python_requires=">=3.7",
    url="https://github.com/SimpleArt/aio",
    author="Jack Nguyen",
    author_email="jackyeenguyen@gmail.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    license_files=["LICENSE"],
)
