import json


def write_json(data, filename='results.json'):
    with open(filename, 'w') as f:
        f.write(json.dumps(data, indent=2, ensure_ascii=False))