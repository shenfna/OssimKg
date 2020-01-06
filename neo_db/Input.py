import pandas as pd


class Input:

    @classmethod
    def csv_to_data(cls, headers, node_type, path):
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
            data['node_attrs'] = attrs
            datas.append(data)
        return datas


if __name__ == '__main__':
    it = Input()
    datas = it.csv_to_data(['id', 'name', 'descr', 'owner'], "host", '../raw_data/map_host_group.csv')
    for data in datas:
        print(data)
