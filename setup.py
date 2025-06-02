from setuptools import setup, find_packages

# Open the README file.
with open(file="README.md", mode="r") as fh:
    long_description = fh.read()

setup(
    name="adf_orchestration",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "azure-identity>=1.12.0",
        "azure-mgmt-datafactory>=9.1.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": ["pytest>=7.0.0"],
    },
    python_requires=">=3.8",
    description="A library for Azure Data Factory orchestration",
    author="Your Name",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)