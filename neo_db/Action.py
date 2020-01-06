from neo_db.create_graph import Importer
from neo_db.Input import Input
from MetaData import Extractor
from PathUtils import get_file_name


class Action:
    # host_headers = ['id', 'hostname', 'asset', 'descr']

    def __init__(self):
        self.mappings = Extractor().get_meta_data()
        self.importer = Importer()
        self.input = Input()

    # def import_node(self, node, path, type):
    #     data = self.input.csv_to_data(node)

    def update_host(self):
        host_mapping = self.mappings['MAP_HOST']
        host_headers = host_mapping['columns']
        hosts = self.input.csv_to_data(headers=host_headers, node_type='Host', path=get_file_name(
                                       file='raw_data/map_host.csv'))
        for host in hosts:
            conditions = {'id': host['node_attrs']['id']}
            self.importer.update_node(host, conditions=conditions)

    def update_host_group(self):
        host_group_mapping = self.mappings['MAP_HOST_GROUP']
        host_group_headers = host_group_mapping['columns']
        host_groups = self.input.csv_to_data(headers=host_group_headers, node_type='HostGroup', path=get_file_name(
            file='raw_data/map_host_group.csv'))
        for host_group in host_groups:
            conditions = {'id': host_group['node_attrs']['id']}
            del host_group['node_attrs']['id']
            self.importer.update_node(host_group, conditions=conditions)

    def main(self):
        self.update_host()
        self.update_host_group()


if __name__ == '__main__':
    action = Action()
    # for i in range(2):
    action.main()
