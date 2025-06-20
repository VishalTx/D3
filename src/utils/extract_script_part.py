import ast
from utils.config_wrapper import ConfigWrapper

def extract_list(script_path, list_name):
    with open(script_path, 'r') as fp:
        tree = ast.parse(fp.read(), filename=script_path)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == list_name:
                    return ast.literal_eval(node.value)
    raise ValueError(f'{list_name} not found in {script_path}')

if __name__ == "__main__":
    config = ConfigWrapper()
    bucket_local_path = config.getenv('Bitbucket_Local_Path')
    script_path = bucket_local_path + '/glue-files/site-packages/standardized_packages/source_code/load_in/trigger_load_in/trigger_load_in.py'
    list_name = 'BASE_CONSUMER_TRIGGER'
    mylist = extract_list(script_path, list_name)
    print(mylist)