import glob
import hashlib
import json
import jsonschema
import os
import pwd

from jupyterhub.spawner import LocalProcessSpawner, set_user_setuid
from subprocess import run, CalledProcessError
from traitlets import default, Dict, Unicode
from urllib.parse import urlparse

def ignite():
    cmd = ['nextflow', 'run', os.environ['NXF_USER_WORKFLOW'], '--PORT={port}', '-resume']

    if 'NXF_USER_REVISION' in os.environ:
        cmd.extend(['-r', os.environ['NXF_USER_REVISION']])
    if 'NXF_USER_PARAMS' in os.environ:
        cmd.extend(['-params-file', os.environ['NXF_USER_PARAMS']])
    if 'NXF_USER_ENDPOINT' in os.environ:
        cmd.extend(['-with-weblog', os.environ['NXF_USER_ENDPOINT']])

    return {
        'command': cmd,
        'timeout': 120,
        'launcher_entry': {'title': 'Nextflow'}
    }

class NextflowSpawner(LocalProcessSpawner):

    default_url = Unicode('/nextflow', help="The entrypoint for the server proxy")

    workflow_url = Unicode(config=True, help="The url of the pipeline repository.")
    workflow_revision = Unicode('main', config=True, help="The revision of the pipeline repository.")

    home_dir_template = Unicode('/home/{username}', config=True, help="Template to expand to set the user home. {username} is expanded to the jupyterhub username.")
    home_dir = Unicode(help="The user home directory")

    log_endpoint = Unicode(None, config=True, allow_none=True, help="The http endpoint for nf-weblog.")

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
        path = f"{self.nxf_home}/assets/{urlparse(self.workflow_url).path[1:]}/nextflow_schema.json"
    
        try:
            run(
                args=['nextflow', 'pull', self.workflow_url, '-r', self.workflow_revision],
                check=True,
                user=self.user.name,
                cwd=self.home_dir,
                env={**os.environ, 'NXF_HOME': self.nxf_home}
            )
            with open(path) as nxf_schema:
                return json.load(nxf_schema)
        except CalledProcessError:
            print(f"{self.workflow_url} does not seem to exist")
        except FileNotFoundError:
            print(f"{self.workflow_url} does not seem to provide a nextflow_schema.json")

    def make_preexec_fn(self, name):
        return set_user_setuid(name, chdir=False)

    def _get_params_from_schema(self, schema, key=None):
        params = {}
        groups = schema['$defs'] if '$defs' in schema else schema['defs']
        for group in groups.values():
            if (param := group.get('properties')).get('type') != 'object':
                for k, v in param.items():
                    params[k] = v if key is None else v.get(key)
            else: # recurse nested parameters
                params[param.get('title')] = self._get_params_from_schema({'$defs': {param.get('title'): param}}, key)
        return params

    def _convert_schema_type(self, type, param=None):
        match type:
            case 'boolean':
                return bool(param) if param is not None else 'checkbox'
            case 'integer':
                return int(param) if param is not None else 'number'
            case 'number':
                return float(param) if param is not None else 'number'
            case _:
                return str(param) if param is not None else 'text'

    def _construct_form_field(self, id, param):
        html = []
        html += "<div class='form-group'>"
        html += "<label for='{id}'>{desc}</label>".format(id=id, desc=param.get('description')) if param.get('description') else []
        match param:
            case {'hidden': _}:  # don't render parameters marked as hidden
                return ""
            case {'enum': enum}: # render enum-style parameters as select list
                html += "<select name='{id}' class='form-control'>".format(id=id)
                for opt in enum:
                    html += "<option value='{opt}'>{opt}</option>".format(id=id, opt=opt)
                html += "</select>"
            case {'type': type, 'default': default}: # render others as input fields
                html += "<input name='{id}' class='form-control' value='{default}' type='{type}'></input>".format(id=id, default=default, type=self._convert_schema_type(type))
            case _: # recurse nested parameters
                html += [ self._construct_form_field(p, v) for p, v in param.items() ]
        html += "</div>"

        return "".join(html)

    def _write_params_file(self, config):
        # dump parameters to json
        json_string = json.dumps(config)

        # generate sha-1 hash from json payload for use as unique filename
        json_sha = hashlib.sha1(json_string.encode()).hexdigest()

        with open(f'{self.nxf_home}/nextflowspawner_{json_sha}.json', 'w', encoding='utf-8') as fout:
            fout.write(json_string)

        return f'{self.nxf_home}/nextflowspawner_{json_sha}.json'

    def _options_form_default(self):
        params = self._get_params_from_schema(self.schema)
        html = ""
        for k, v in params.items():
            html += self._construct_form_field(k, v)
        return html

    def options_from_form(self, formdata):
        # get types and defaults from schema
        types, defaults = self._get_params_from_schema(self.schema, 'type'), self._get_params_from_schema(self.schema, 'default')

        # get user-defined parameters from form and cast types
        params = { k: self._convert_schema_type(types.get(k), v.pop()) for k, v in formdata.items() }

        # check if provided paths exist and permissions suffice
        for param, format in self._get_params_from_schema(self.schema, 'format').items():
            if (pattern := params.get(param)) and format == 'path':
                if not (paths := glob.glob(pattern)):
                    raise FileNotFoundError(f"{pattern} does not exist.")
                if not os.access(os.path.dirname(pattern), os.R_OK):
                    raise PermissionError(f"Parent directory is not readable.")
                if (not_readable := [path for path in paths if not os.access(path, os.R_OK)]):
                    raise PermissionError(f"{not_readable} are not readable.")

        # update defaults with user-defined parameters from form
        options = defaults | params

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
        return env
