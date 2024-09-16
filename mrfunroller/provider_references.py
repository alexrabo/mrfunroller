# my_mrf_parser/provider_references.py

import ijson

def parse_provider_references(json_file_path):
    provider_references = {}

    with open(json_file_path, 'r') as file:
        parser = ijson.parse(file)
        for prefix, event, value in parser:
            if prefix == 'provider_references.item' and event == 'start_map':
                current_group_id = None
                current_providers = []
            elif prefix.endswith('provider_group_id') and event == 'number':
                current_group_id = value
            elif prefix.endswith('provider_groups.item') and event == 'start_map':
                current_provider = {'npi': [], 'tin': {}}
            elif prefix.endswith('npi.item') and event == 'number':
                current_provider['npi'].append(value)
            elif prefix.endswith('tin.type') and event == 'string':
                current_provider['tin']['type'] = value
            elif prefix.endswith('tin.value') and event == 'string':
                current_provider['tin']['value'] = value
            elif prefix.endswith('provider_groups.item') and event == 'end_map':
                current_providers.append(current_provider)
            elif prefix == 'provider_references.item' and event == 'end_map':
                provider_references[current_group_id] = current_providers

    return provider_references
