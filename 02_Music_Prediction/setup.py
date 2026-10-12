from setuptools import find_packages, setup

MODEL_EXTRAS = {
    "neural": ["lightning>=2.2"],
    "graph": ["lightning>=2.2", "torch-geometric>=2.5"],
    "lightgbm": ["lightgbm>=4.0"],
    "tabpfn": ["tabpfn>=9.1,<10"],
}

setup(
    name="situation-prediction",
    version="0.1.0",
    python_requires=">=3.10",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        "numpy>=1.26",
        "pandas>=2.2",
        "scipy>=1.11",
        "scikit-learn>=1.4",
        "torch>=2.6",
        "joblib>=1.3",
        "pyreadr>=0.5",
        "hydra-core>=1.3.2",
        "omegaconf>=2.3",
    ],
    extras_require={
        **MODEL_EXTRAS,
        "all": sorted({requirement for requirements in MODEL_EXTRAS.values() for requirement in requirements}),
        "tracking": ["wandb>=0.16"],
        "plots": ["matplotlib>=3.8", "seaborn>=0.13"],
        "explain": ["shap>=0.45"],
        "test": ["pytest>=8.0"],
    },
)
