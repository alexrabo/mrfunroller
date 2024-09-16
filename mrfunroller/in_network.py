# my_mrf_parser/in_network.py

import ijson
from collections import defaultdict
import json
import os

def parse_in_network(json_file_path, output_file_path):
    pointer = None  # Keep track of the last processed tuple
    processed_count = 0
    accumulated_data = []  # To accumulate results in memory

    with open(json_file_path, 'r') as file:
        parser = ijson.parse(file)
        current_pointer = pointer

        grouped_entries = defaultdict(lambda: {
            'billing_code_type': None,
            'billing_code': None,
            'provider_group_ids': []
        })

        for prefix, event, value in parser:
            # Skip to the pointer position if it exists
            if current_pointer and prefix < current_pointer:
                continue

            if prefix == 'in_network.item' and event == 'start_map':
                item = parse_single_in_network(parser)
                processed_item = process_in_network_item(item)

                if processed_item is not None:
                    billing_code_type, billing_code, name, description, provider_group_ids = processed_item
                    grouped_key = (name, description)
                    grouped_entries[grouped_key]['billing_code_type'] = billing_code_type
                    grouped_entries[grouped_key]['billing_code'] = billing_code
                    grouped_entries[grouped_key]['provider_group_ids'].extend(provider_group_ids)
                    processed_count += 1
                    provlist = sorted(list(set(provider_group_ids)), key=lambda x: float(x))

                    accumulated_data.append({
                        'name': name,
                        'description': description,
                        'billing_code_type': billing_code_type,
                        'billing_code': billing_code,
                        'provider_group_ids': provlist
                    })
                current_pointer = prefix    

            # Save results to file when a batch of entries is accumulated
            if processed_count >= 100:
                save_to_file(accumulated_data, output_file_path)
                accumulated_data.clear()
                processed_count = 0

    if accumulated_data:
        save_to_file(accumulated_data, output_file_path)

def parse_single_in_network(parser):
    current_entry = {
        'negotiation_arrangement': None,
        'billing_code_type': None,
        'billing_code': None,
        'name': None,
        'description': None,
        'provider_group_ids': set()
    }

    for prefix, event, value in parser:
        if prefix.endswith('negotiation_arrangement') and event == 'string':
            current_entry['negotiation_arrangement'] = value
        elif prefix.endswith('billing_code_type') and event == 'string':
            current_entry['billing_code_type'] = value
        elif prefix.endswith('billing_code') and event == 'string':
            current_entry['billing_code'] = value
        elif prefix.endswith('name') and event == 'string':
            current_entry['name'] = value
        elif prefix.endswith('description') and event == 'string':
            current_entry['description'] = value
        elif prefix.endswith('negotiated_rates.item.provider_references.item') and event == 'number':
            group_ids = str(value) if isinstance(value, (int, float)) else value
            current_entry['provider_group_ids'].add(group_ids)

        if prefix == 'in_network.item' and event == 'end_map':
            break

    return current_entry

def process_in_network_item(entry):
    if all(entry.get(key) is not None for key in ['billing_code_type', 'billing_code', 'name', 'description']):
        return (
            entry.get('billing_code_type'),
            entry.get('billing_code'),
            entry.get('name'),
            entry.get('description'),
            list(entry.get('provider_group_ids', []))
        )
    return None

def save_to_file(data, output_file_path):
    with open(output_file_path, 'a') as f:  # Append mode to add data incrementally
        for entry in data:
            json.dump(entry, f)
            f.write('\n')

def read_output_file_line_by_line(output_file_path):
    try:
        with open(output_file_path, 'r') as file:
            for line in file:
                if line.strip():
                    data = json.loads(line)
                    print(data)
    except FileNotFoundError:
        print(f"The file {output_file_path} does not exist.")
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
    finally:
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
            print(f"The file {output_file_path} has been deleted.")
        else:
            print(f"The file {output_file_path} was not found for deletion.")
