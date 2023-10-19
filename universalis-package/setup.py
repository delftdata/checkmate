import setuptools

setuptools.setup(
    name="universalis",
    version="0.0.1",
    author="Kyriakos Psarakis",
    packages=setuptools.find_packages(),
    install_requires=[
        'cloudpickle>=3.0.0,<4.0.0',
        'msgspec>=0.18.4,<1.0.0',
        'aiokafka>=0.8.1,<1.0',
    ],
    python_requires='>=3.11',
)
