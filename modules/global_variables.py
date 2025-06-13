from yaml import load, Loader

# Load the params.yaml file
params = load(open('modules/params.yaml').read(), Loader=Loader)
