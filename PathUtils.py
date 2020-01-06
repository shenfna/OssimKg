import os


def get_file_name(file):
    root = os.path.dirname(os.path.realpath(__file__))  # 获取项目根目录
    path = os.path.join(root, file)
    return path
