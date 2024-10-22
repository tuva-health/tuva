import json
import csv
import argparse
import logging
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_latest_release(repo):
    url = f'https://api.github.com/repos/{repo}/releases/latest'
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()

def get_file_from_release(repo, tag_name, file_path):

    file_url = f'https://raw.githubusercontent.com/{repo}/{tag_name}/{file_path}'

    response = requests.get(file_url)
    response.raise_for_status()  # Raise an error for bad responses

    logging.debug(f"Retrieved {file_path} from release {tag_name} ")
    logging.debug(response.content.decode('utf-8'))
    return response.content.decode('utf-8')


def process_node(node_id, node_data,  manifest, rows, tuva_rows, skipped_nodes, undocumented_columns, version):
    schema = node_data['metadata']['schema']
    table = node_data['metadata']['name']
    columns = node_data['columns']
    manifest_node_data = manifest['nodes'].get(node_id, {})


    logging.debug(f"Processing node: {node_id} | Schema: {schema} | Table: {table}")

    package_name = node_id.split('.')[1]
    test = 1

    if package_name.lower() not in ['tuva', 'the_tuva_project']  and node_id.startswith('seed') :
        skipped_nodes.append(node_id)
        logging.debug(f"skipping input layer seed {node_id}")
    elif package_name.lower() in ['tuva', 'the_tuva_project'] and node_id.startswith('model') and ( 'final' not in manifest_node_data['path'] or 'clinical_concept_library__value_set_member_relevant_fields' in node_data['columns'] or 'medical_claim_expanded' in node_data['columns'] ):
        skipped_nodes.append(node_id)
        logging.debug(f"skipping non-final node \n -{node_id} \n -{manifest_node_data['path']}")

    else:
        for col_name, col_data in columns.items():
            column_name = col_name.lower()
            data_type = col_data['type']
            ordinal_number = col_data['index']

            description = manifest['nodes'].get(node_id, {}).get('columns', {}).get(column_name, {}).get('description', '')
            if not description:
                logging.debug(f"No description found in catalog.json for {schema}.{table}.{column_name}")
                undocumented_columns.append(node_id+'-'+column_name)

            row = {
                'Schema': schema,
                'Table': table,
                'Column': col_name,
                'Data Type': data_type,
                'Ordinal Number': ordinal_number,
                'Description': description,
                'Version': version
            }


            if package_name.lower() in ['tuva', 'the_tuva_project']:
                logging.debug(f"Writing to Tuva Project CSV: {schema}.{table}.{column_name}")
                tuva_rows.append(row)
            else:
                # logging.info(node_id)
                logging.debug(f"Writing to Input Layer CSV: {schema}.{table}.{column_name}")
                rows.append(row)


def main(tag):
    repo = 'tuva-health/tuva'
    if not tag:
        release_info = get_latest_release(repo)
        tag_name = release_info['tag_name']
    else:
        tag_name = tag

    logging.info(f"Building tables from {tag_name}")
    catalog_str = get_file_from_release(repo, tag_name, 'docs/catalog.json')
    manifest_str = get_file_from_release(repo, tag_name, 'docs/manifest.json')

    catalog = json.loads(catalog_str)
    manifest = json.loads(manifest_str)
    input_rows = []
    tuva_rows = []
    skipped_nodes = []
    undocumented_columns = []

    for node_id, node_data in catalog['nodes'].items():
        process_node(node_id, node_data, manifest, input_rows, tuva_rows, skipped_nodes, undocumented_columns, tag_name)

    input_rows.sort(key=lambda x: (x['Schema'], x['Table'], x['Ordinal Number']))
    tuva_rows.sort(key=lambda x: (x['Schema'], x['Table'], x['Ordinal Number']))


    input_csv_path =  'input_layer_'+tag_name+'.csv'
    tuva_csv_path = 'tuva_project_'+tag_name+'.csv'

    fieldnames = ['Schema', 'Table', 'Column', 'Data Type', 'Ordinal Number', 'Description', 'Version']

    logging.info(f"Writing Input Layer CSV to {input_csv_path}")
    with open(input_csv_path, 'w', newline='') as input_csv_file:
        input_writer = csv.DictWriter(input_csv_file, fieldnames=fieldnames)
        input_writer.writeheader()
        input_writer.writerows(input_rows)

    logging.info(f"Writing Tuva Project CSV to {tuva_csv_path}")
    with open(tuva_csv_path, 'w', newline='') as tuva_csv_file:
        tuva_writer = csv.DictWriter(tuva_csv_file, fieldnames=fieldnames)
        tuva_writer.writeheader()
        tuva_writer.writerows(tuva_rows)

    logging.info(f"CSV files created: {input_csv_path}, {tuva_csv_path}")
    skipped_nodes.sort()
    for skipped_node in skipped_nodes:
        print(skipped_node)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process dbt catalog and manifest JSON files to generate CSVs.')
    parser.add_argument('release', nargs='?',
                        help='Release tag (default: latest)')
    args = parser.parse_args()
    logging.debug(f'arguments: {args}')
    main(args.release)
