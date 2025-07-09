import pystache
from os import path
import minify_html

def build_index(out_dir, base_url, repository_url, metadata):
    """
    Build the index page using the provided metadata.
    """
    with open("templates/index.html", 'r', encoding='utf-8') as f:
        template = f.read()

    renderer = pystache.Renderer()
    rendered = renderer.render(template, {'metadata': metadata, 'repository_url': repository_url, 'base_url': base_url})
    rendered = minify_html.minify(rendered, minify_js=True, minify_css=True, remove_processing_instructions=True)

    with open(path.join(out_dir, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(rendered)