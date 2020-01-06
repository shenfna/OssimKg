import pandas as pd


class Output:

    @classmethod
    def data_to_csv(cls, data, columns, path):
        data = list(data)
        columns = list(columns)
        file_data = pd.DataFrame(data, index=range(len(data)), columns=columns)
        file_data.to_csv(path, index=False)
