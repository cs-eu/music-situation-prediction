from setuptools import setup, find_packages

setup(
    name='situation-prediction',
    version='0.1.0',  # Use a string for the version
    packages=find_packages(where="src"),
    package_dir={"": "src"},  # Map the root package to the `src` directory
    include_package_data=True,
    install_requires=[
        # List your dependencies here, e.g.:
        "numpy",
        "pandas",
        "scikit-learn",
    ],
)