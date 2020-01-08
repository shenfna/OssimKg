import pandas as pd


class Input:

    @classmethod
    def csv_to_node_data(cls, headers, orient_headers, node_type, path):
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
                data['orient_headers'] = orient_headers
                datas.append(data)
        for data in datas:
            print(data)
        return datas

    @classmethod
    def csv_to_relation_data(cls, out_node_data, in_node_data, orient_headers, relation_name, path):
        datas = []
        out_headers = out_node_data['headers']
        in_heasers = in_node_data['headers']
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
            for head in in_heasers:
                if pd.isna(row[head]):
                    continue
                in_attrs[head] = row[head]
            in_node['node_attrs'] = in_attrs
            if in_attrs and out_attrs:
                data['out_node'] = out_node
                data['in_node'] = in_node
                data['relation_name'] = relation_name
                data['orient_headers'] = orient_headers
                datas.append(data)
        for data in datas:
            print(data)
        return datas


if __name__ == '__main__':
    it = Input()
    # datas = it.csv_to_node_data(['id', 'name', 'descr', 'owner'], "host", '../raw_data/map_host_group.csv')
    # for data in datas:
    #     print(data)
    # datas = it.csv_to_relation_data(out_node_data={'type': 'Host', 'headers': ['host_id']},
    #                                 in_node_data={'type': 'HostGroup', 'headers': ['host_group_id']},
    #                                 relation_name='hasGroup', path='../raw_data/map_host_group_ref.csv')
    datas = it.csv_to_node_data(headers=['descr'], orient_headers={'descr': 'descr'},
                                node_type='Host', path='../raw_data/map_host.csv')
    for data in datas:
        print(data)
