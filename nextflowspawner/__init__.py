import hashlib
import json
import jsonschema

from jupyterhub.spawner import LocalProcessSpawner

class NextflowSpawner(LocalProcessSpawner):
    
    def make_preexec_fn(self, name):
        pass

    default_url = '/nextflow' # entrypoint for https://github.com/phue/jupyter-nextflow-proxy

    @property
    def schema(self, schema_path='nextflow_schema.json'):
        with open(schema_path) as j:
            return json.load(j)   

    def _get_params_from_schema(self, schema, key=None):
        params = {}
        for group in schema['defs'].values():
            if (param := group.get('properties')).get('type') != 'object':
                for k, v in param.items():
                    params[k] = v if key is None else v.get(key)
            else: # recurse nested parameters
                params[param.get('title')] = self._get_params_from_schema({'defs': {param.get('title'): param}}, key)
        return params

    def _construct_form_field(self, id, param):
        form_types = {'boolean': 'radio', 'string': 'text', 'integer': 'number', 'number': 'number'}
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
                html += "<input name='{id}' class='form-control' placeholder='{default}' type='{type}'></input>".format(id=id, default=default, type=form_types.get(type))
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

        # get default parameters, then update with user-defined parameters from form
        options = self._get_params_from_schema(self.schema, 'default') | { k: v.pop() for k, v in formdata.items() }

        # validate against schema
        jsonschema.validate(options, schema = self.schema)

        return options


    def get_env(self):
        env = super().get_env()
        env['NXF_PARAMS_FILE'] = self._write_params_file(self.user_options)
        return env