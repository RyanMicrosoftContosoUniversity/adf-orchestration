from setuptools import setup, find_packages

# Open the README file.
with open(file="README.md", mode="r") as fh:
    long_description = fh.read()

setup(
    name='adf_orchestration',
    version='0.1.0',
    install_requires=[
        'azure-mgmt-datafactory',
        'azure-identity',
        'pytest'
        
    ],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            # Define command-line scripts here, e.g.,
            # 'metadata-scan=scripts.metadata_scan:main',
        ],
    },
    include_package_data=True,
    description='A framework for orchestrating azure data factory pipelines',
    author='RH',
    author_email='RH@gmail.com',
    url='https://github.com/RyanMicrosoftContosoUniversity/adf-orchestration',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
)