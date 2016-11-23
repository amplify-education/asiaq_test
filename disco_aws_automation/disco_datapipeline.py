"""
Back end behaviors for creating data pipelines.
"""

import json
from datetime import datetime
from logging import getLogger

import boto3

from .disco_config import open_normalized
from .resource_helper import throttled_call


_LOG = getLogger(__name__)


class DataPipelineConsts(object):
    """Constants for data pipeline management."""
    TEMPLATE_DIR = "datapipeline_templates"


class AsiaqDataPipeline(object):
    """Local encapsulation of the information associated with a single data pipeline."""
    def __init__(self, name, description, tags=None, metadata=None,
                 pipeline_id=None, contents=None, parameter_definitions=None, param_values=None):
        self._objects = contents
        self._params = parameter_definitions
        self._param_values = _optional_dict_to_list(param_values)
        self._name = name
        self._description = description
        self._tags = _optional_dict_to_list(tags, key_string='key', value_string='value')
        self._metadata = metadata
        self._id = pipeline_id

    def is_persisted(self):
        "Return true if this pipeline has an AWS ID; false otherwise."
        return self._id is not None

    def has_content(self):
        "Return true if this pipeline has actual pipeline objects, false if it is only metadata."
        return self._objects is not None

    def update_content(self, contents, parameter_definitions, param_values):
        "Set the pipeline content (pipeline nodes, parameters and values) for this pipeline."
        self._objects = contents
        self._params = parameter_definitions
        self._param_values = param_values

    @classmethod
    def from_template(cls, template_name, name, description, tags=None, param_values=None, log_location=None):
        """Create a new AsiaqDataPipeline object, populated from template in the configuration directory."""
        with open_normalized(DataPipelineConsts.TEMPLATE_DIR, template_name + ".json") as f:
            template_data = json.load(f)
        boto_objects, boto_parameters = template_to_boto(template_data)
        if log_location:
            add_log_location_param(boto_objects, log_location)
        return cls(contents=boto_objects, parameter_definitions=boto_parameters,
                   name=name, description=description, tags=tags, param_values=param_values)


class AsiaqDataPipelineManager(object):
    "List, retrieve, store and delete pipelines."
    def __init__(self, client=None):
        self._dp_client = client or boto3.client("datapipeline")

    def save(self, pipeline):
        "Save or update the pipeline object in AWS."
        if not pipeline.is_persisted():  # if this is a new pipeline, save the metadata first
            unique_id = "pipeline_init_%s" % datetime.now().isoformat()  # make the create call idempotent
            created = throttled_call(self._dp_client.create_pipeline,
                                     name=pipeline._name, uniqueId=unique_id,
                                     description=pipeline._description, tags=pipeline._tags or [])
            pipeline._id = created['pipelineId']
        # regardless, save the pipeline content:
        resp = throttled_call(self._dp_client.put_pipeline_definition,
                              pipelineId=pipeline._id,
                              pipelineObjects=pipeline._objects or [],
                              parameterObjects=pipeline._params or [],
                              parameterValues=pipeline._param_values or [])
        return resp

    # possibly worth having but not currently needed: set_tags
    # http://boto3.readthedocs.io/en/latest/reference/services/datapipeline.html#DataPipeline.Client.add_tags

    def fetch(self, pipeline_id):
        "Fetch a pipeline (metadata and content) from AWS by ID."
        contents = throttled_call(self._dp_client.get_pipeline_definition, pipelineId=pipeline_id,
                                  version='latest')
        meta_resp = throttled_call(self._dp_client.describe_pipelines, pipelineIds=[pipeline_id])
        meta = meta_resp['pipelineDescriptionList'][0]
        return AsiaqDataPipeline(
            pipeline_id=pipeline_id, name=meta['name'], description=meta.get('description'),
            tags=meta.get('tags'), metadata=meta.get('fields'),
            contents=contents['pipelineObjects'], parameter_definitions=contents['parameterObjects'],
            param_values=contents['parameterValues'],
        )

    def fetch_content(self, pipeline):
        "Populate the pipeline content fields of this pipeline object with the information fetched from AWS."
        if pipeline.has_content():
            raise Exception("Content already fetched or locally generated for this pipeline object")
        definition = throttled_call(self._dp_client.get_pipeline_definition,
                                    pipelineId=pipeline._id, version='latest')
        pipeline.update_content(definition['pipelineObjects'],
                                definition['parameterObjects'],
                                definition['parameterValues'])

    def fetch_all_descriptions(self):
        """
        Fetch all pipelines in this account/region, populating only their name/metadata fields
        (use fetch_content to get the low-level details).
        """
        id_objects = self._fetch_ids()
        descriptions = []
        window = 25
        for i in range(0, len(id_objects), window):
            batch = throttled_call(self._dp_client.describe_pipelines,
                                   pipelineIds=[desc['id'] for desc in id_objects[i:i + window]])
            descriptions.extend(batch['pipelineDescriptionList'])
        return [
            AsiaqDataPipeline(
                pipeline_id=meta['pipelineId'], name=meta['name'], description=meta.get('description'),
                tags=meta.get('tags'), metadata=meta.get('fields'))
            for meta in descriptions
        ]

    def start(self, pipeline, params=None):
        "Activate the pipeline in AWS."
        if not pipeline.is_persisted():
            raise Exception("Pipeline must be saved before it can be activated")
        param_values = _optional_dict_to_list(params) or pipeline._param_values
        return self._dp_client.activate_pipeline(pipelineId=pipeline._id, startTimestamp=datetime.now(),
                                                 parameterValues=param_values)

    def stop(self, pipeline):
        "Deactivate the pipeline in AWS."
        if not pipeline.is_persisted():
            raise Exception("Pipeline must be saved before it can be deactivated")
        return self._dp_client.deactivate_pipeline(pipelineId=pipeline._id)

    def delete(self, pipeline):
        "Remove the pipeline completely from AWS."
        if not pipeline.is_persisted():
            raise Exception("Pipeline must be saved before it can be deleted (though honestly...)")
        self._dp_client.delete_pipeline(pipelineId=pipeline._id)

    def _fetch_ids(self):
        found_defs = []
        resp = throttled_call(self._dp_client.list_pipelines)
        found_defs.extend(resp['pipelineIdList'])
        while resp['hasMoreResults']:
            resp = throttled_call(self._dp_client.list_pipelines, marker=resp['marker'])
            found_defs.extend(resp['pipelineIdList'])
        return found_defs


def template_to_boto(template_json):
    "Transform data in the format that Amazon gives in their templates to the format boto3 uses."
    boto3_objects = []
    boto3_params = []
    conserved_object_fields = ['id', 'name']
    for obj in template_json['objects']:
        xformed = {k: obj[k] for k in conserved_object_fields if k in obj}
        fields = []
        for key, value in obj.items():
            if key in conserved_object_fields:
                continue
            if isinstance(value, dict) and 'ref' in value:
                fields.append({'key': key, 'refValue': value['ref']})
            elif isinstance(value, (list, tuple)):
                fields.extend([{'key': key, 'stringValue': step} for step in value])
            else:
                fields.append({'key': key, 'stringValue': value})
        xformed['fields'] = fields
        boto3_objects.append(xformed)

    for param in template_json['parameters']:
        xformed = {k: param[k] for k in conserved_object_fields if k in param}
        xformed['attributes'] = [{'key': k, 'stringValue': param[k]}
                                 for k in param
                                 if k not in conserved_object_fields]
        boto3_params.append(xformed)
    return (boto3_objects, boto3_params)


def add_log_location_param(boto_objects, log_location):
    """
    Silly-looking utility function to traverse a pipeline template, find the place where we want
    to insert the URI for logs to be written, and insert it there.  Abstracted out to keep it from
    cluttering up more interesting code.
    """
    default_found = False
    for pipeline_obj in boto_objects:
        if pipeline_obj['name'] == 'Default':
            default_found = True
            log_setting_found = False
            for field in pipeline_obj['fields']:
                if field['key'] == 'pipelineLogUri':
                    log_setting_found = True
                    _LOG.debug("Updating existing log bucket %s to %s", field['stringValue'], log_location)
                    field['stringValue'] = log_location
            if not log_setting_found:
                pipeline_obj['fields'].append({'key': 'pipelineLogUri', 'stringValue': log_location})
            break
    if not default_found:
        raise Exception("No 'Default' object found: this is probably not a valid data pipeline definition")


def _optional_dict_to_list(param_value_dict, key_string='id', value_string='stringValue'):
    """
    If given a dictionary, convert it to a list of key-value dictionary entries.
    If not given a dictionary, just return whatever we were given.
    """
    if not isinstance(param_value_dict, dict):
        return param_value_dict
    value_objects = []
    for param_id, value in param_value_dict.items():
        value_objects.append({key_string: param_id, value_string: value})
    return value_objects
