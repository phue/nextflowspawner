import glob
import hashlib
import json
import jsonschema
import os
import pwd

from jupyterhub.spawner import LocalProcessSpawner
from subprocess import run, CalledProcessError
from traitlets import default, Dict, Unicode
from urllib.parse import urlparse

class NextflowSpawner(LocalProcessSpawner):

    @property
    def nxf_home(self):
        return os.getenv('NXF_HOME', f"{pwd.getpwnam(self.user.name).pw_dir}/.nextflow")

    default_url = Unicode('/nextflow', help="entrypoint for https://github.com/phue/jupyter-nextflow-proxy")
    workflow_url = Unicode(config=True, help="The url of the pipeline repository.")
    
    schema = Dict(config=True, help="The pipeline JSON schema.")

    @default('schema')
    def _default_schema(self):
        path = f"{self.nxf_home}/assets/{urlparse(self.workflow_url).path[1:]}/nextflow_schema.json"
    
        try:
            run(['nextflow', 'pull', self.workflow_url], check=True)
            with open(path) as nxf_schema:
                return json.load(nxf_schema)
        except CalledProcessError:
            print(f"{self.workflow_url} does not seem to exist")
        except FileNotFoundError:
            print(f"{self.workflow_url} does not seem to provide a nextflow_schema.json")

    def make_preexec_fn(self, _):
        pass

    def _get_params_from_schema(self, schema, key=None):
        params = {}
        for group in schema['defs'].values():
            if (param := group.get('properties')).get('type') != 'object':
                for k, v in param.items():
                    params[k] = v if key is None else v.get(key)
            else: # recurse nested parameters
                params[param.get('title')] = self._get_params_from_schema({'defs': {param.get('title'): param}}, key)
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

        with open(f'{json_sha}.json', 'w', encoding='utf-8') as fout:
            fout.write(json_string)

        return f'{json_sha}.json'

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

        # check if provided paths actually exist
        for k, v in self._get_params_from_schema(self.schema, 'format').items():
            if k in params.keys() and v == 'path':
                if not (g := glob.glob(params.get(k))):
                    raise FileNotFoundError(f"{params.get(k)} does not exist.")
                else:
                    for p in g:
                        if not os.access(p, os.R_OK):
                            raise PermissionError(f"{p} is not readable.")

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
        env['NXF_HOME'] = self.nxf_home
        env['NXF_USER_WORKFLOW'] = self.workflow_url
        env['NXF_USER_PARAMS'] = self._write_params_file(self.user_options)
        return env