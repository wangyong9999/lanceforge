from setuptools import setup, find_packages

setup(
    name="lanceforge",
    version="0.1.0",
    description="Python SDK for LanceForge distributed multimodal retrieval engine",
    long_description=open("README.md").read() if __import__("os").path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    url="https://github.com/wangyong9999/lanceforge",
    license="Apache-2.0",
    packages=find_packages(),
    package_data={
        "lanceforge": ["*.py"],  # include generated pb2 files
    },
    python_requires=">=3.9",
    install_requires=[
        "grpcio>=1.50",
        "protobuf>=4.0",
        "pyarrow>=12.0",
    ],
    extras_require={
        "langchain": ["langchain-core>=0.1"],
        "numpy": ["numpy"],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
)
