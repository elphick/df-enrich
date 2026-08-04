# Configuration file for the Sphinx documentation builder.

import os
import sys
sys.path.insert(0, os.path.abspath('../src'))
import df_enrich

# -- Project information -----------------------------------------------------
project = 'df-enrich'
copyright = "2026, Greg Elphick"
author = "Greg Elphick"
version = df_enrich.__version__

# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx_autodoc_typehints',
    'myst_parser',
    'sphinx_gallery.gen_gallery',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
html_theme = 'sphinx_book_theme'
html_static_path = []

html_theme_options = {
    "repository_url": "https://github.com/elphick/df-enrich",
    "use_repository_button": True,
}
# Use project branding (icons) for the docs
html_logo = "../assets/branding/logo.svg"
html_favicon = "../assets/branding/logo.svg"

html_theme_options = {
    "repository_url": "https://github.com/elphick/df-enrich",
    "use_repository_button": True,
    "use_issues_button": True,
    "use_edit_page_button": True,
    "path_to_docs": "docs",
    "repository_branch": "main",
    "logo": {
        "image_light": "../assets/branding/logo.svg",
        "image_dark": "../assets/branding/logo.svg",
        "text": f"df-enrich<br>({version})",  # shows version in the top-left

    },
}

# -- Extension configuration -------------------------------------------------
# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True

# Intersphinx mapping
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'pandas': ('https://pandas.pydata.org/docs/', None),
    'pandera': ('https://pandera.readthedocs.io/en/stable/', None),
}

# Autodoc settings
autodoc_member_order = 'bysource'
autodoc_typehints = 'description'
autosummary_generate = True

# Sphinx-Gallery settings
sphinx_gallery_conf = {
    'examples_dirs': '../examples',
    'gallery_dirs': 'auto_examples',
    'filename_pattern': r'.*\.py',
    'doc_module': ('df_enrich',),
    'reference_url': {'df_enrich': None},
}
