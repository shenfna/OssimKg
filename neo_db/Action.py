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

    # def insert_node(self, node, path, type):
    #     data = self.input.csv_to_data(node)

    def update_host(self):
        host_mapping = self.mappings['MAP_HOST']
        host_headers = host_mapping['columns']
        host_ori_heads = dict()
        for head in host_headers:
            host_ori_heads[head] = head
        hosts = self.input.csv_to_node_data(headers=host_headers, orient_headers=host_ori_heads, node_type='Host',
                                            path=get_file_name(file='raw_data/map_host.csv'))
        for host in hosts:
            condition = {'id': host['node_attrs']['id']}
            host['condition'] = condition
        self.importer.update_node(hosts)

    def update_host_group(self):
        host_group_mapping = self.mappings['MAP_HOST_GROUP']
        host_group_headers = host_group_mapping['columns']
        host_group_ori_heads = dict()
        for head in host_group_headers:
            host_group_ori_heads[head] = head
        host_groups = self.input.csv_to_node_data(headers=host_group_headers, orient_headers=host_group_ori_heads,
                                                  node_type='HostGroup',
                                                  path=get_file_name(file='raw_data/map_host_group.csv'))
        for host_group in host_groups:
            condition = {'id': host_group['node_attrs']['id']}
            # del host_group['node_attrs']['id']
            host_group['condition'] = condition
        self.importer.update_node(host_groups)

    def insert_host_group_ref(self):
        # host_group_ref_mapping = self.mappings['MAP_HOST_GROUP_REF']
        # host_group_ref_headers = host_group_ref_mapping['columns']
        out_node_data = dict()
        out_node_data['headers'] = ['host_id']
        out_node_data['type'] = 'Host'
        in_node_data = dict()
        in_node_data['headers'] = ['host_group_id']
        in_node_data['type'] = 'HostGroup'
        host_group_ref_ori_heads = {'host_id': 'id', 'host_group_id': 'id'}
        host_group_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                         relation_name='hasGroup',
                                                         orient_headers=host_group_ref_ori_heads,
                                                         path=get_file_name(file='raw_data/map_host_group_ref.csv'))
        self.importer.insert_relation(host_group_ref)

    def main(self):
        self.update_host()
        self.update_host_group()
        self.insert_host_group_ref()


if __name__ == '__main__':
    action = Action()
    # for i in range(2):
    action.main()
