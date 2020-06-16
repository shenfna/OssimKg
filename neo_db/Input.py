import pandas as pd


class Input:

    @classmethod
    def csv_to_node_data(cls, headers, node_type, path):
        datas = []
        df = pd.read_csv(path)
        for index, row in df.iterrows():
            data = dict()
            data['node_type'] = node_type
            attrs = dict()
            for head in headers:
                if pd.isna(row[head]):
                    continue
                attrs[head] = row[head]
            if attrs:
                data['node_attrs'] = attrs
                datas.append(data)
        for data in datas:
            print(data)
        return datas

    @classmethod
    def csv_to_relation_data(cls, out_node_data, in_node_data, relation_name, relation_pro, path):
        datas = []
        out_headers = out_node_data['headers']
        in_headers = in_node_data['headers']
        out_type = out_node_data['type']
        in_type = in_node_data['type']
        df = pd.read_csv(path)
        for index, row in df.iterrows():
            data = dict()
            out_node = dict()
            out_node['node_type'] = out_type
            out_attrs = dict()
            for head in out_headers:
                if pd.isna(row[head]):
                    continue
                out_attrs[head] = row[head]
            out_node['node_attrs'] = out_attrs
            in_node = dict()
            in_node['node_type'] = in_type
            in_attrs = dict()
            for head in in_headers:
                if pd.isna(row[head]):
                    continue
                in_attrs[head] = row[head]
            in_node['node_attrs'] = in_attrs
            relation_attrs = dict()
            if relation_pro:
                for pro in relation_pro:
                    if pd.isna(row[pro]):
                        continue
                    relation_attrs[pro] = row[pro]
            if in_attrs and out_attrs:
                data['out_node'] = out_node
                data['in_node'] = in_node
                data['relation_name'] = relation_name
                data['relation_attrs'] = relation_attrs
                datas.append(data)
        for data in datas:
            print(data)
        return datas

    # alias[0] out_alias, alias[1] in_alias, alias[2] relation_alias
    @classmethod
    def csv_to_relation_data_with_alias(cls, out_node_data, in_node_data, relation_name, relation_pro, alias, path):
        datas = []
        out_headers = out_node_data['headers']
        in_headers = in_node_data['headers']
        out_type = out_node_data['type']
        in_type = in_node_data['type']
        df = pd.read_csv(path)
        for index, row in df.iterrows():
            data = dict()
            out_node = dict()
            out_node['node_type'] = out_type
            out_attrs = dict()
            for head in out_headers:
                if pd.isna(row[head]):
                    continue
                if alias[0] and head in alias[0]:
                    out_attrs[alias[0][head]] = row[head]
                else:
                    out_attrs[head] = row[head]
            out_node['node_attrs'] = out_attrs
            in_node = dict()
            in_node['node_type'] = in_type
            in_attrs = dict()
            for head in in_headers:
                if pd.isna(row[head]):
                    continue
                if pd.isna(row[head]):
                    continue
                if alias[1] and head in alias[1]:
                    in_attrs[alias[1][head]] = row[head]
                else:
                    in_attrs[head] = row[head]
            in_node['node_attrs'] = in_attrs
            relation_attrs = dict()
            if relation_pro:
                for pro in relation_pro:
                    if pd.isna(row[pro]):
                        continue
                    if alias[2] and pro in alias[2]:
                        relation_attrs[alias[2][pro]] = row[pro]
                    else:
                        relation_attrs[pro] = row[pro]
            if in_attrs and out_attrs:
                data['out_node'] = out_node
                data['in_node'] = in_node
                data['relation_name'] = relation_name
                data['relation_attrs'] = relation_attrs
                datas.append(data)
        for data in datas:
            print(data)
        return datas


if __name__ == '__main__':
    it = Input()
    # datas = it.csv_to_node_data(['id', 'name', 'descr', 'owner'], "host", '../raw_data/map_host_group.csv')
    # for data in datas:
    #     print(data)
    datas = it.csv_to_relation_data(out_node_data={'type': 'Attacker', 'headers': ['attackerIP','attackerPort']},
                                    in_node_data={'type': 'Victim', 'headers': ['victimIP','victimPort']},
                                    relation_name='attacker', path='../raw_data/map_event.csv',relation_pro=[])
    # datas = it.csv_to_node_data(headers=['hostId','name','asset','descr','created','updated'],
    #                             node_type='Host', path='../raw_data/map_host.csv')
