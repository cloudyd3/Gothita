from setuptools import setup, find_packages

setup(
    name="container-tracker",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyyaml>=6.0",
        "aiodocker>=0.38",
        "kubernetes-asyncio>=30.0",
        "motor>=3.6",
        "aiokafka>=0.10",
        "aiohttp>=3.11",
    ],
    entry_points={
        "console_scripts": [
            "container-tracker=main:main",
        ],
    },
)
