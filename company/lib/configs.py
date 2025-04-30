import os

envs = {
    'DOCKER_APP' : os.environ.get('DOCKER_APP', False),
    'HEADLESS' : os.environ.get('HEADLESS', False),
    'MONGO_STRING' : os.environ.get('MONGO_STRING', "mongodb+srv://rtj:rtjadmin@cluster0.maxbdey.mongodb.net/"),
    'DATABASE_NAME' : os.environ.get('DATABASE_NAME', 'e-finder'),
}