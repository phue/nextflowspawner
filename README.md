# nextflowspawner

Spawn nextflow pipelines from Jupyter and configure them interactively.
Works best together with https://github.com/phue/jupyter-nextflow-proxy.

:warning: This requires the pipeline to come with a [nextflow_schema.json](https://nextflow-io.github.io/nf-validation/latest/nextflow_schema/nextflow_schema_specification/)
:warning: For now, the spawner needs to be started from the pipeline root directory.