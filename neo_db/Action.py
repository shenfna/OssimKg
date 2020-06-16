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
        # hostId,hostname,asset,descr,created,updated
        host_mapping = self.mappings['MAP_HOST']
        host_headers = host_mapping['columns']
        hosts = self.input.csv_to_node_data(headers=host_headers, node_type='Host',
                                            path=get_file_name(file='raw_data/map_host.csv'))
        primary_keys = ['hostId']
        # for host in hosts:
        #     condition = dict()
        #     if 'id' in host['node_attrs'].keys():
        #         condition['id'] = host['node_attrs']['id']
        #         host['condition'] = condition
        self.importer.update_node(hosts, primary_keys)

    def update_host_group(self):
        # hostGroupId,name,descr,owner
        host_group_mapping = self.mappings['MAP_HOST_GROUP']
        host_group_headers = host_group_mapping['columns']
        host_groups = self.input.csv_to_node_data(headers=host_group_headers, node_type='HostGroup',
                                                  path=get_file_name(file='raw_data/map_host_group.csv'))
        primary_keys = ['hostGroupId']
        self.importer.update_node(host_groups, primary_keys)

    def update_host_group_ref(self):
        # host_group_ref_mapping = self.mappings['MAP_HOST_GROUP_REF']
        # host_group_ref_headers = host_group_ref_mapping['columns']
        out_node_data = dict()
        out_node_data['headers'] = ['hostId']
        out_node_data['type'] = 'Host'
        in_node_data = dict()
        in_node_data['headers'] = ['hostGroupId']
        in_node_data['type'] = 'HostGroup'
        host_group_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                         relation_name='hasGroup',
                                                         path=get_file_name(file='raw_data/map_host_group_ref.csv'),relation_pro=[])
        self.importer.update_relation(host_group_ref)

    def update_host_os(self):
        # name
        host_os = self.input.csv_to_node_data(headers=['name'], node_type='OS',
                                              path=get_file_name(file='raw_data/map_host_os.csv'))
        primary_keys = ['name']
        # 插入OS节点
        self.importer.update_node(nodes=host_os, primary_keys=primary_keys)

        out_node_data = dict()
        out_node_data['headers'] = ['hostId']
        out_node_data['type'] = 'Host'
        in_node_data = dict()
        in_node_data['headers'] = ['name']
        in_node_data['type'] = 'OS'
        host_os_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                      relation_name='hasAsset',
                                                      path=get_file_name(file='raw_data/map_host_os.csv'),relation_pro=[])
        # 插入Host和OS的边
        self.importer.update_relation(host_os_ref)

        out_node_data.clear()
        in_node_data.clear()
        out_node_data['headers'] = ['name']
        out_node_data['type'] = 'OS'
        in_node_data['headers'] = ['sourceId']
        in_node_data['type'] = 'Source'
        os_source_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                        relation_name='foundBy',
                                                        path=get_file_name(file='raw_data/map_host_os.csv'),relation_pro=[])
        #   插入OS和Source的边
        self.importer.update_relation(os_source_ref)

    def update_host_source(self):
        # sourceId,name
        host_source_mapping = self.mappings['MAP_HOST_SOURCE']
        host_source_headers = host_source_mapping['columns']
        host_sources = self.input.csv_to_node_data(headers=host_source_headers, node_type='Source',
                                                   path=get_file_name(file='raw_data/map_host_source.csv'))
        primary_keys = ['sourceId']
        self.importer.update_node(host_sources, primary_keys)

    def update_host_ip(self):
        host_ip_mapping = self.mappings['MAP_HOST_IP']
        host_ip_headers = host_ip_mapping['columns']
        host_ip = self.input.csv_to_node_data(headers=host_ip_headers, node_type='Host',
                                              path=get_file_name(file='raw_data/map_host_ip.csv'))
        primary_keys = ['hostId']
        self.importer.update_node(host_ip, primary_keys)

    def update_host_service(self):
        # hostIp,port,name
        host_service_headers = ['port', 'name']
        host_service = self.input.csv_to_node_data(headers=host_service_headers, node_type='Service',
                                                   path=get_file_name(file='raw_data/map_host_service.csv'))
        primary_keys = ['port', 'name']
        # 插入Service节点
        self.importer.update_node(nodes=host_service, primary_keys=primary_keys)

        # hostId,name
        out_node_data = dict()
        out_node_data['headers'] = ['hostId']
        out_node_data['type'] = 'Host'
        in_node_data = dict()
        in_node_data['headers'] = ['name', 'port']
        in_node_data['type'] = 'Service'
        host_service_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                           relation_name='hasAsset',
                                                           path=get_file_name(file='raw_data/map_host_service.csv'),relation_pro=[])
        # 插入Host和Service的边
        self.importer.update_relation(host_service_ref)

        out_node_data.clear()
        in_node_data.clear()
        out_node_data['headers'] = ['name', 'port']
        out_node_data['type'] = 'Service'
        in_node_data['headers'] = ['sourceId']
        in_node_data['type'] = 'Source'
        service_source_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                             relation_name='foundBy',
                                                             path=get_file_name(file='raw_data/map_host_service.csv'),relation_pro=[])
        #   插入Service和Source的边
        self.importer.update_relation(service_source_ref)

    def update_host_software(self):
        # cpe,banner
        host_software_headers = ['name']
        host_software = self.input.csv_to_node_data(headers=host_software_headers,
                                                    node_type='Software',
                                                    path=get_file_name(file='raw_data/map_host_software.csv'))
        # 插入Software节点
        self.importer.update_node(nodes=host_software, primary_keys=host_software_headers)

        # host_id,cpe
        out_node_data = dict()
        out_node_data['headers'] = ['hostId']
        out_node_data['type'] = 'Host'
        in_node_data = dict()
        in_node_data['headers'] = ['name']
        in_node_data['type'] = 'Software'
        host_software_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                            relation_name='hasAsset',
                                                            path=get_file_name(file='raw_data/map_host_software.csv'),relation_pro=[])
        # 插入Host和Software的边
        self.importer.update_relation(host_software_ref)

        out_node_data.clear()
        in_node_data.clear()
        out_node_data['headers'] = ['name']
        out_node_data['type'] = 'Software'
        in_node_data['headers'] = ['sourceId']
        in_node_data['type'] = 'Source'
        software_source_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                              relation_name='foundBy',
                                                              path=get_file_name(file='raw_data/map_host_software.csv'),relation_pro=[])
        #   插入Service和Source的边
        self.importer.update_relation(software_source_ref)

    def update_alarm(self):
        alarm_attacker_headers = ['srcIP', 'srcPort']
        alarm_attacker = self.input.csv_to_node_data(headers=alarm_attacker_headers, node_type='Attacker',
                                                     path=get_file_name(file='raw_data/map_alarm.csv'))
        primary_keys = alarm_attacker_headers
        # 插入Attacker
        self.importer.update_node(alarm_attacker, primary_keys)

        alarm_victim_headers = ['dstIP', 'dstPort']
        alarm_victim = self.input.csv_to_node_data(headers=alarm_victim_headers, node_type='Victim',
                                                   path=get_file_name(file='raw_data/map_alarm.csv'))
        primary_keys = alarm_victim_headers
        # 插入Victim
        self.importer.update_node(alarm_victim, primary_keys)

        out_node_data = dict()
        out_node_data['headers'] = alarm_attacker_headers
        out_node_data['type'] = 'Attacker'
        in_node_data = dict()
        in_node_data['headers'] = alarm_victim_headers
        in_node_data['type'] = 'Victim'
        attacker_victim_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                              relation_name='attack',
                                                              path=get_file_name(file='raw_data/map_alarm.csv'),relation_pro=[])
        # 插入Attacker和Victim的边
        self.importer.update_relation(attacker_victim_ref)

    def update_event(self):
        event_attacker_headers = ['attackerIP', 'attackerPort', 'attackerAsset', 'attackerName', 'attackerMac',
                                  'attackerHost', 'attackerNet']
        event_attacker = self.input.csv_to_node_data(headers=event_attacker_headers, node_type='Attacker',
                                                     path=get_file_name(file='raw_data/map_event.csv'))
        primary_keys = ['attackerIP', 'attackerPort']
        # 插入Attacker
        self.importer.update_node(event_attacker, primary_keys)

        event_victim_headers = ['victimIP', 'victimPort', 'victimAsset', 'victimName', 'victimMac', 'victimHost',
                                'victimNet']
        event_victim = self.input.csv_to_node_data(headers=event_victim_headers, node_type='Victim',
                                                   path=get_file_name(file='raw_data/map_event.csv'))
        primary_keys = ['victimIP', 'victimPort']
        # 插入Victim
        self.importer.update_node(event_victim, primary_keys)

        out_node_data = dict()
        out_node_data['headers'] = ['attackerIP', 'attackerPort']
        out_node_data['type'] = 'Attacker'
        in_node_data = dict()
        in_node_data['headers'] = ['victimIP', 'victimPort']
        in_node_data['type'] = 'Victim'
        attacker_victim_ref = self.input.csv_to_relation_data(out_node_data=out_node_data, in_node_data=in_node_data,
                                                              relation_name='attack',
                                                              path=get_file_name(file='raw_data/map_event.csv'),
                                                              relation_pro=['timestamp','eventName','eventType'])
        # 插入Attacker和Victim的边
        self.importer.update_relation(attacker_victim_ref)


    def main(self):
        # self.update_host()
        # self.update_host_group()
        # self.update_host_group_ref()
        # self.update_host_source()
        # self.update_host_os()
        # self.update_host_ip()
        # self.update_host_service()
        # self.update_host_software()
        self.update_event()

# self.update_alarm()


if __name__ == '__main__':
    action = Action()
    # for i in range(2):
    action.main()
