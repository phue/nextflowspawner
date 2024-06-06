import setuptools

setuptools.setup(
  name="nextflowspawner",
  version='0.5.0',
  url="https://github.com/phue/nextflowspawner",
  description="Spawn Nextflow pipelines from Jupyterhub and configure them interactively",  
  author="Patrick Hüther",
  keywords=['Jupyter', 'Nextflow'],
  classifiers=['Framework :: Jupyter'],
  entry_points={
    'jupyterhub.spawners': [
        'nextflow = nextflowspawner:NextflowSpawner',
    ],
  },
  python_requires=">=3.10",
  install_requires=['jsonschema', 'jupyterhub>=4.0.2', 'jupyter-nextflow-proxy@git+https://github.com/phue/jupyter-nextflow-proxy@v0.3.0'],
)
