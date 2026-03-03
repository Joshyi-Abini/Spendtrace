from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="spendtrace",
    version="1.1.0",
    author="Cost Attribution Contributors",
    author_email="maintainers@cost-attribution.dev",
    description="Feature-level cloud spend attribution for Python applications",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/spendtrace/spendtrace",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=[
        "psutil>=5.9.0",
        "python-dateutil>=2.8.0",
    ],
    extras_require={
        "api": ["fastapi>=0.100.0", "uvicorn[standard]>=0.23.0", "pydantic>=2.0.0"],
        "dashboard": ["jinja2>=3.1.0"],
        "timescaledb": ["psycopg2-binary>=2.9.0"],
        "dynamic-pricing": ["boto3>=1.34.0"],
        "influxdb": ["influxdb-client>=1.36.0"],
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "mypy>=1.4.0",
            "ruff>=0.0.280",
        ],
    },
    entry_points={
        "console_scripts": [
            "spendtrace=cost_attribution.cli.main:main",
            "cost-attribution=cost_attribution.cli.main:main",
        ],
    },
)
