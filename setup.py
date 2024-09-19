import setuptools

setuptools.setup(
  name="nextflowspawner",
  version='0.8.2',
  url="https://github.com/phue/nextflowspawner",
  description="Spawn Nextflow pipelines from Jupyterhub and configure them interactively",  
  author="Patrick Hüther",
  keywords=['Jupyter', 'Nextflow'],
  classifiers=['Framework :: Jupyter'],
  entry_points={
    'jupyter_serverproxy_servers': [
        'nextflow = nextflowspawner:ignite'
    ],
    'jupyterhub.spawners': [
        'nextflow = nextflowspawner:NextflowSpawner',
    ],
  },
  python_requires=">=3.10",
  install_requires=['jsonschema', 'jupyterhub>=5.0.0', 'jupyter-server-proxy'],
)
