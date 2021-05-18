import setuptools

setuptools.setup(
    name="spa_de_exercise",
    version="0.1",
    description="Sportalliance Data Engineer exercise",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
)