from setuptools import setup, find_packages

VERSION = '0.1.18'
DESCRIPTION = 'IceCreamSwap Web3.py wrapper'
LONG_DESCRIPTION = 'IceCreamSwap Web3.py wrapper with automatic retries, multicall and other advanced functionality'

requirements = [
    'web3',
    'rlp',
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
    keywords=['python', 'IceCreamSwapWeb3'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ],
    package_data={
        # Ensure this path reflects the structure inside your package
        'IceCreamSwapWeb3': ['abi/*.abi', 'bytecode/*.bytecode'],
    },
)
