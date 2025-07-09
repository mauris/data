from processors.hawker_center_closure import hawker_center_closure
from utils.cache import save_cache
from utils.html import build_index
import json

base_url = "https://mauris.github.io/data"
repository_url = "https://github.com/mauris/data"

processors = [
    {
        'func': hawker_center_closure,
        'name': 'Hawker Center Closures',
        'data_category': 'hawker-closures',
        'description': 'Calendars of hawker center and market closures in Singapore, sorted by area within 2km of MRT stations, with information provided from NEA via data.gov.sg.'
    }
]

if __name__ == "__main__":
    out_dir = "output/"

    metadata = []
    for p in processors:
        processor_meta = {
            'name': p['name'],
            'data_category': p['data_category'],
            'description': p['description'],
            'files': []
        }
        for file in p['func']():
            file.write(out_dir, p['data_category'])
            processor_meta['files'].append(file.filename)
        processor_meta['files'] = sorted(processor_meta['files'])
        metadata.append(processor_meta)
    
    json.dump(metadata, open(f"{out_dir}/metadata.json", "w"), indent=2)
    build_index(out_dir, base_url, repository_url, metadata)
    save_cache()  # Ensure cache is saved after processing