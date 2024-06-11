import os


def project_path(path):
    this_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.join(this_dir, "../")
    return os.path.join(project_dir, path)
