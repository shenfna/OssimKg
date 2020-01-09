from neo_db.create_graph import Importer
from neo_db.Input import Input
from MetaData import Extractor
from PathUtils import get_file_name


class Action:

    def __init__(self):
        self.mappings = Extractor().get_meta_data()
        self.importer = Importer()
        self.input = Input()

    # def insert_node(self, node, path, type):
    #     data = self.input.csv_to_data(node)

    def update_host(self):
        # id,hostname,asset,descr,created,updated
        host_mapping = self.mappings['MAP_HOST']
        host_headers = host_mapping['columns']
        host_ori_heads = dict()
        for head in host_headers:
            host_ori_heads[head] = head
        host_ori_heads['hostname'] = 'name'
        hosts = self.input.csv_to_node_data(headers=host_headers, orient_headers=host_ori_heads, node_type='Host',
                                            path=get_file_name(file='raw_data/map_host.csv'))
        for host in hosts:
            condition = dict()
            if 'id' in host['node_attrs'].keys():
                condition['id'] = host['node_attrs']['id']
                host['condition'] = condition
        self.importer.update_node(hosts)

    def update_host_group(self):
        # id,name,descr,owner
        host_group_mapping = self.mappings['MAP_HOST_GROUP']
        host_group_headers = host_group_mapping['columns']
        host_group_ori_heads = dict()
        for head in host_group_headers:
            host_group_ori_heads[head] = head
        host_groups = self.input.csv_to_node_data(headers=host_group_headers, orient_headers=host_group_ori_heads,
                                                  node_type='HostGroup',
                                                  path=get_file_name(file='raw_data/map_host_group.csv'))
        for host_group in host_groups:
            condition = dict()
            if 'id' in host_group['node_attrs'].keys():
                condition['id'] = host_group['node_attrs']['id']
                # del host_group['node_attrs']['id']
                host_group['condition'] = condition
        self.importer.update_node(host_groups)

    def update_host_group_ref(self):
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

    def update_host_os(self):
        # name
        host_os = self.input.csv_to_node_data(headers=['value'], orient_headers={'value': 'name'},
                                              node_type='OS',
                                              path=get_file_name(file='raw_data/map_host_os.csv'))
        for os in host_os:
            condition = dict()
            if 'value' in os['node_attrs'].keys():
                condition['value'] = os['node_attrs']['value']
                os['condition'] = condition
        # 插入OS节点
        self.importer.update_node(nodes=host_os)

        out_node_data = dict()
        out_node_data['headers'] = ['host_id']
        out_node_data['type'] = 'Host'
        in_node_data = dict()
        in_node_data['headers'] = ['value']
        in_node_data['type'] = 'OS'
        host_os_ori_heads = {'host_id': 'id', 'value': 'name'}
        host_os_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                      relation_name='hasAsset',
                                                      orient_headers=host_os_ori_heads,
                                                      path=get_file_name(file='raw_data/map_host_os.csv'))
        # 插入Host和OS的边
        self.importer.update_relation(host_os_ref)

        out_node_data.clear()
        in_node_data.clear()
        host_os_ori_heads.clear()
        out_node_data['headers'] = ['value']
        out_node_data['type'] = 'OS'
        in_node_data['headers'] = ['source_id']
        in_node_data['type'] = 'Source'
        host_os_ori_heads = {'value': 'name', 'source_id': 'id'}
        os_source_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                        relation_name='foundBy',
                                                        orient_headers=host_os_ori_heads,
                                                        path=get_file_name(file='raw_data/map_host_os.csv'))
    #   插入OS和Source的边
        self.importer.update_relation(os_source_ref)

    def update_host_source(self):
        # id,name
        host_source_mapping = self.mappings['MAP_HOST_SOURCE']
        host_source_headers = host_source_mapping['columns']
        host_source_ori_heads = dict()
        for head in host_source_headers:
            host_source_ori_heads[head] = head
        host_source_ori_heads['name'] = 'name'
        host_sources = self.input.csv_to_node_data(headers=host_source_headers, orient_headers=host_source_ori_heads,
                                                   node_type='Source',
                                                   path=get_file_name(file='raw_data/map_host_source.csv'))
        for source in host_sources:
            condition = dict()
            if 'id' in source['node_attrs'].keys():
                condition['id'] = source['node_attrs']['id']
                source['condition'] = condition
        self.importer.update_node(host_sources)

    def update_host_ip(self):
        host_ip_mapping = self.mappings['MAP_HOST_IP']
        host_ip_headers = host_ip_mapping['columns']
        host_ip_ori_heads = {'host_id': 'id', 'ip': 'ip'}
        host_ip = self.input.csv_to_node_data(headers=host_ip_headers, orient_headers=host_ip_ori_heads,
                                              node_type='Host', path=get_file_name(file='raw_data/map_host_ip.csv'))
        for host in host_ip:
            condition = dict()
            if 'host_id' in host['node_attrs'].keys():
                condition['host_id'] = host['node_attrs']['host_id']
                host['condition'] = condition
        self.importer.update_node_property(host_ip)

    def update_host_service(self):
        # host_ip,port,service,version
        host_service_headers = ['port', 'service', 'version']
        host_service_ori_heads = {'port': 'port', 'service': 'name', 'version': 'version'}
        host_service = self.input.csv_to_node_data(headers=host_service_headers, orient_headers=host_service_ori_heads,
                                                   node_type='Service',
                                                   path=get_file_name(file='raw_data/map_host_service.csv'))
        for service in host_service:
            condition = dict()
            keys = service['node_attrs'].keys()
            if ('service' in keys) or ('version' in keys) or ('port' in keys):
                if 'service' in keys:
                    condition['service'] = service['node_attrs']['service']
                else:
                    condition['service'] = ''
                if 'version' in keys:
                    condition['version'] = service['node_attrs']['version']
                else:
                    condition['version'] = ''
                if 'port' in keys:
                    condition['port'] = service['node_attrs']['port']
                else:
                    condition['port'] = ''
                service['condition'] = condition
        # 插入Service节点
        self.importer.update_node(nodes=host_service)

        # host_id,service,version
        out_node_data = dict()
        out_node_data['headers'] = ['host_id']
        out_node_data['type'] = 'Host'
        in_node_data = dict()
        in_node_data['headers'] = ['service', 'version', 'port']
        in_node_data['type'] = 'Service'
        host_service_ori_heads = {'host_id': 'id', 'service': 'name', 'version': 'version', 'port': 'port'}
        host_service_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                           relation_name='hasAsset',
                                                           orient_headers=host_service_ori_heads,
                                                           path=get_file_name(file='raw_data/map_host_service.csv'))
        # 插入Host和Service的边
        self.importer.insert_relation(host_service_ref)

        out_node_data.clear()
        in_node_data.clear()
        host_service_ori_heads.clear()
        out_node_data['headers'] = ['service', 'version', 'port']
        out_node_data['type'] = 'Service'
        in_node_data['headers'] = ['source_id']
        in_node_data['type'] = 'Source'
        host_service_ori_heads = {'service': 'name', 'version': 'version', 'source_id': 'id', 'port': 'port'}
        service_source_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                             relation_name='foundBy',
                                                             orient_headers=host_service_ori_heads,
                                                             path=get_file_name(file='raw_data/map_host_service.csv'))
        #   插入Service和Source的边
        self.importer.update_relation(service_source_ref)

    def update_host_software(self):
        # cpe,banner
        host_software_headers = ['cpe', 'banner']
        host_software_ori_heads = {'cpe': 'cpe', 'banner': 'name'}
        host_software = self.input.csv_to_node_data(headers=host_software_headers,
                                                    orient_headers=host_software_ori_heads,
                                                    node_type='Software',
                                                    path=get_file_name(file='raw_data/map_host_software.csv'))
        for software in host_software:
            condition = dict()
            keys = software['node_attrs'].keys()
            if 'cpe' in keys:
                condition['cpe'] = software['node_attrs']['cpe']
                software['condition'] = condition
        # 插入Software节点
        self.importer.update_node(nodes=host_software)

        # host_id,cpe
        out_node_data = dict()
        out_node_data['headers'] = ['host_id']
        out_node_data['type'] = 'Host'
        in_node_data = dict()
        in_node_data['headers'] = ['cpe']
        in_node_data['type'] = 'Software'
        host_software_ori_heads = {'host_id': 'id', 'cpe': 'cpe'}
        host_software_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                            relation_name='hasAsset',
                                                            orient_headers=host_software_ori_heads,
                                                            path=get_file_name(file='raw_data/map_host_software.csv'))
        # 插入Host和Software的边
        self.importer.insert_relation(host_software_ref)

        out_node_data.clear()
        in_node_data.clear()
        host_software_ori_heads.clear()
        out_node_data['headers'] = ['cpe']
        out_node_data['type'] = 'Software'
        in_node_data['headers'] = ['source_id']
        in_node_data['type'] = 'Source'
        host_software_ori_heads = {'cpe': 'cpe', 'source_id': 'id'}
        software_source_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                              relation_name='foundBy',
                                                              orient_headers=host_software_ori_heads,
                                                              path=get_file_name(file='raw_data/map_host_software.csv'))
        #   插入Service和Source的边
        self.importer.update_relation(software_source_ref)

    def update_host_type(self):
        # host_id,name
        host_type_mapping = self.mappings['MAP_HOST_TYPE']
        host_type_headers = host_type_mapping['columns']
        host_type_ori_heads = {'host_id': 'id', 'name': 'type'}
        host_type = self.input.csv_to_node_data(headers=host_type_headers, orient_headers=host_type_ori_heads,
                                                node_type='Host', path=get_file_name(file='raw_data/map_host_type.csv'))
        for host in host_type:
            condition = dict()
            if 'host_id' in host['node_attrs'].keys():
                condition['host_id'] = host['node_attrs']['host_id']
                host['condition'] = condition
        self.importer.update_node_property(host_type)

    def update_host_subtype(self):
        # host_id,name
        host_subtype_mapping = self.mappings['MAP_HOST_SUBTYPE']
        host_subtype_headers = host_subtype_mapping['columns']
        host_subtype_ori_heads = {'host_id': 'id', 'name': 'subtype'}
        host_subtype = self.input.csv_to_node_data(headers=host_subtype_headers, orient_headers=host_subtype_ori_heads,
                                                   node_type='Host',
                                                   path=get_file_name(file='raw_data/map_host_subtype.csv'))
        for host in host_subtype:
            condition = dict()
            if 'host_id' in host['node_attrs'].keys():
                condition['host_id'] = host['node_attrs']['host_id']
                host['condition'] = condition
        self.importer.update_node_property(host_subtype)

    def main(self):
        self.update_host()
        self.update_host_group()
        self.update_host_group_ref()
        self.update_host_source()
        self.update_host_os()
        self.update_host_ip()
        self.update_host_service()
        self.update_host_software()
        self.update_host_type()
        self.update_host_subtype()


if __name__ == '__main__':
    action = Action()
    # for i in range(2):
    action.main()
