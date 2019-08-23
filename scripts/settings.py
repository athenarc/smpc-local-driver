import os
from dotenv import load_dotenv
from utils import read_json

ENV_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../.env')
load_dotenv(dotenv_path=ENV_PATH)

NODE_ENV = os.getenv('NODE_ENV')
USE_CATALOGUE = os.getenv('USE_CATALOGUE')
CATALOGUE_API = os.getenv('CATALOGUE_API')
CATALOGUE_EXPLORER_API = '{0}/{1}'.format(CATALOGUE_API, 'catalogue')
CATALOGUE_MESH_API = '{0}/{1}'.format(CATALOGUE_API, 'transmesh')
REQUEST_DIRECTORY = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../requests')
DATASET_DIRECTORY = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../datasets')
SMPC_GLOBAL_DIRECTORY = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../smpc-global/')

MESH_BY_CODES = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'meshTermsByCode.json'))
MESH_TERMS = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'meshTerms.json'))
MAPPING = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'mapping.json'))
MESH_INVERSED = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'meshTermsInversed.json'))
KEYWORDS = read_json(os.path.join(SMPC_GLOBAL_DIRECTORY, 'keywords.json'))
