from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'IceCreamSwap Web3.py wrapper'
LONG_DESCRIPTION = 'IceCreamSwap Web3.py wrapper with automatic retries and advanced functionality'

requirements = [
    'web3',
]

# Setting up
setup(
    name="IceCreamSwapWeb3",
    version=VERSION,
    author="IceCreamSwap",
    author_email="",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=requirements,
    # needs to be installed along with your package.

    keywords=['python', 'IceCreamSwapWeb3'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
