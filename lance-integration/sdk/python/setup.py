from setuptools import setup, find_packages

setup(
    name="lanceforge-client",
    version="0.1.0",
    description="Python SDK for LanceForge distributed multimodal retrieval",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "grpcio>=1.50",
        "pyarrow>=12.0",
    ],
    extras_require={
        "langchain": ["langchain-core>=0.1"],
    },
)
