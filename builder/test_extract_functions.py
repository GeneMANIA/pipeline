
import glob
import extract_functions

def test():

    input_files = glob.glob('ontologies/processed/*.txt')

    extract_functions.extract_function_groups(input_files, "ONTOLOGIES.txt")
    extract_functions.extract_functions(input_files, "ONTOLOGIES.txt", "ONTOLOGY_CATEGORIES.txt")