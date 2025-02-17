import glob
import hashlib
import json
import os
import pwd
from subprocess import CalledProcessError, run
from urllib.parse import urlparse

import jsonschema
from jupyterhub.spawner import LocalProcessSpawner, set_user_setuid
from traitlets import Dict, Unicode, default


def ignite():
    """
    Launch a Nextflow pipeline instance via jupyter-server-proxy.
    """

    cmd = ['nextflow', 'run', os.environ['NXF_USER_WORKFLOW'], '--SOCKET={unix_socket}', '-resume']

    if 'NXF_USER_REVISION' in os.environ:
        cmd.extend(['-r', os.environ['NXF_USER_REVISION']])
    if 'NXF_USER_PARAMS' in os.environ:
        cmd.extend(['-params-file', os.environ['NXF_USER_PARAMS']])
    if 'NXF_USER_ENDPOINT' in os.environ:
        cmd.extend(['-with-weblog', os.environ['NXF_USER_ENDPOINT']])
    if 'NXF_USER_PROFILE' in os.environ:
        cmd.extend(['-profile', os.environ['NXF_USER_PROFILE']])

    return {
        'command': cmd,
        'timeout': 120,
        'launcher_entry': {'title': 'Nextflow'},
        'unix_socket': True,
        'raw_socket_proxy': True
    }

class NextflowSpawner(LocalProcessSpawner):
    """
    A Spawner for Nextflow pipelines.
    """

    default_url = Unicode('/nextflow', help="The entrypoint for the server proxy")

    workflow_url = Unicode(config=True, help="The url of the pipeline repository.")
    workflow_revision = Unicode('main', config=True, help="The revision of the pipeline repository.")

    home_dir_template = Unicode('/home/{username}', config=True, help="Template to expand to set the user home. {username} is expanded to the jupyterhub username.")
    home_dir = Unicode(help="The user home directory")

    log_endpoint = Unicode(None, config=True, allow_none=True, help="The http endpoint for nf-weblog.")
    nxf_profile = Unicode(None, config=True, allow_none=True, help="Nextflow profile(s) to use for pipeline execution.")

    @default('home_dir')
    def _default_home_dir(self):
        return self.home_dir_template.format(username=self.user.name)

    nxf_home = Unicode(help="The directory where nextflow assets are stored.")

    @default('nxf_home')
    def _default_nxf_home(self):
        return os.getenv('NXF_HOME', f"{self.home_dir}/.nextflow")

    nxf_launch = Unicode(help="The directory where the pipeline is launched.")

    @default('nxf_launch')
    def _default_nxf_launch(self):
        path = f"{self.home_dir}/{self.workflow_url.split('/').pop()}"
        if not os.path.exists(path):
            os.makedirs(path)
            os.chown(path, pwd.getpwnam(self.user.name).pw_uid, pwd.getpwnam(self.user.name).pw_uid)
        return path

    popen_kwargs = Dict(help="Extra keyword arguments to pass to Popen.")

    @default('popen_kwargs')
    def _default_popen_kwargs(self):
        return {'cwd': self.nxf_launch}

    schema = Dict(config=True, help="The pipeline JSON schema.")

    @default('schema')
    def _default_schema(self):
        schema_path = os.path.join(self.nxf_home, 'assets', urlparse(self.workflow_url).path[1:], 'nextflow_schema.json')
        try:
            run(
                args=['nextflow', 'pull', self.workflow_url, '-r', self.workflow_revision],
                check=True,
                user=self.user.name,
                cwd=self.home_dir,
                env={**os.environ, 'NXF_HOME': self.nxf_home}
            )
            with open(schema_path) as nxf_schema:
                return json.load(nxf_schema)
        except CalledProcessError:
            msg = f"{self.workflow_url} does not seem to exist"
            self.log.exception(msg)
        except FileNotFoundError:
            msg = f"{self.workflow_url} does not seem to provide a nextflow_schema.json"
            self.log.exception(msg)

    def make_preexec_fn(self, name):
        return set_user_setuid(name, chdir=False)

    def _get_params_from_schema(self, schema, key=None):
        params_dict = {}
        groups = schema['$defs'] if '$defs' in schema else schema['defs']
        for group, defs in groups.items():
            params_dict[group] = {}
            for param, properties in defs.get('properties').items():
                if properties.get('type') != 'object':
                    params_dict[group][param] = properties if key is None else properties.get(key)
                else:
                    # recurse nested parameters
                    params_dict[group] |= self._get_params_from_schema({'$defs': {param: {**properties}}}, key)
        return params_dict

    def _construct_form_field(self, name, param):
        html = []
        match param:
            case {'hidden': _}:
                pass
            case {'type': ptype, 'description': description, 'default': default}:
                html += f"<label for='{name}'>{description}</label>"
                if choices := param.get('enum'):
                    # render enums as select list
                    html += f"<select name='{name}' class='form-control'>"
                    for opt in choices:
                        html += f"<option value='{opt}'>{opt}</option>"
                    html += "</select>"
                else:
                    # render input fields dependent on parameter type
                    match ptype:
                        case 'integer' | 'number':
                            html += f"<input name='{name}' class='form-control' value='{default}' type='number'></input>"
                        case 'string':
                            html += f"<input name='{name}' class='form-control' value='{default}' type='text'></input>"
                        case 'boolean':
                            html += f"<input name='{name}' class='form-control' value='{default}' type='checkbox'></input>"
                # add help text if available
                if help_text := param.get('help_text'):
                    html += f"<small class='form-text text-muted'>{help_text}</small>"
            case _:
                # recurse nested parameters
                nested = []
                for p, v in param.items():
                    nested += self._construct_form_field(p, v)
                if nested:
                    html += nested
        return html

    def _write_params_file(self, config):
        # dump parameters to json
        json_string = json.dumps(config)

        # generate sha-1 hash from json payload for use as unique filename
        json_sha = hashlib.sha1(json_string.encode()).hexdigest()

        with open(f'{self.nxf_home}/nextflowspawner_{json_sha}.json', 'w', encoding='utf-8') as fout:
            fout.write(json_string)

        return f'{self.nxf_home}/nextflowspawner_{json_sha}.json'

    def _options_form_default(self):
        form = []
        for group, params in self._get_params_from_schema(self.schema).items():
            if category := self._construct_form_field(group, params):
                # this only renders card if category contains atleast one non-hidden parameter
                form += "<div class='card'>"
                form += f"<div class='card-header'>{group} options</div>"
                form += "<div class='card-body'>"
                form += category
                form += "</div></div>"
        return "".join(form)

    def options_from_form(self, formdata):
        def _cast_schema_type(ptype, param):
            match ptype:
                case 'boolean':
                    return bool(param)
                case 'integer':
                    return int(param)
                case 'number':
                    return float(param)
                case _:
                    return str(param)

        def _apply_form_params(params, formdata):
            params_dict = {}
            for param, properties in params.items():
                if 'type' not in properties:
                    # recurse nested parameters
                    return {param: _apply_form_params(properties, formdata)}

                value = _cast_schema_type(properties.get('type'), formdata.get(param, [properties.get('default')]).pop(0))

                # check if file(s) exists and permissions suffice
                if 'exists' in properties:
                    if not glob.glob(value):
                        msg = f"{value} does not exist."
                        raise FileNotFoundError(msg)
                    if not os.access(os.path.dirname(value), os.R_OK):
                        msg = "{value} is not readable."
                        raise PermissionError(msg)

                params_dict[param] = value

            return params_dict

        options = {}

        # apply user-defined parameters from form, use defaults if not provided
        for param in self._get_params_from_schema(self.schema).values():
            options |= _apply_form_params(param, formdata)

        # add email address for notifications (if provided via config)
        if 'NXF_USER_EMAIL' in self.environment:
            options['EMAIL'] = self.environment['NXF_USER_EMAIL']

        # validate against schema
        jsonschema.validate(options, schema=self.schema)

        return options

    def get_env(self):
        env = super().get_env()
        env['HOME'] = self.home_dir
        env['NXF_HOME'] = self.nxf_home
        env['NXF_USER_WORKFLOW'] = self.workflow_url
        env['NXF_USER_REVISION'] = self.workflow_revision
        env['NXF_USER_PARAMS'] = self._write_params_file(self.user_options)
        if self.log_endpoint:
            env['NXF_USER_ENDPOINT'] = self.log_endpoint
        if self.nxf_profile:
            env['NXF_USER_PROFILE'] = self.nxf_profile
        return env
