import os
from dotenv import load_dotenv
from utils import read_json

ENV_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../.env')
load_dotenv(dotenv_path=ENV_PATH)

CATALOGUE_API = os.getenv('CATALOGUE_API')
CATALOGUE_EXPLORER_API = CATALOGUE_API + "catalogue_explorer"
CATALOGUE_MESH_API = CATALOGUE_API + "transmesh"
REQUEST_DIRECTORY = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../requests')
SMPC_GLOBAL_DIRECTORY = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../smpc-global/')

MESH_BY_CODES = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'meshTermsByCode.json'))
MESH_TERMS = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'meshTerms.json'))
MAPPING = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'mapping.json'))
MESH_INVERSED = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'meshTermsInversed.json'))
KEYWORDS = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'keywords.json'))
