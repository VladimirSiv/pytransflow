import os
import sys
from datetime import datetime

current_date = datetime.now()
sys.path.insert(0, os.path.abspath("../.."))
project = "pytransflow"
copyright = f"{current_date.year}, Vladimir Sivcevic"
author = "Vladimir Sivcevic"
release = "0.1.0"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
]

source_suffix = {
    ".rst": "restructuredtext",
}

napoleon_use_param = True

templates_path = ["_templates"]
html_theme = "sphinx_book_theme"
