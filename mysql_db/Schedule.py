from mysql_db.Database import DatabaseConn
from mysql_db.Output import Output
from MetaData import Extractor
from PathUtils import get_file_name


class Schedule:
    def __init__(self):
        self.extractor = Extractor()
        self.database = DatabaseConn()
        self.output = Output()
        self.main()

    def main(self):
        mappings = self.extractor.get_meta_data()
        files = []
        for key, val in mappings.items():
            name = key
            columns = val['columns']
            source = val['source']
            data = self.database.exec_query(source)
            file = dict()
            file['path'] = get_file_name(file='raw_data/' + name.strip().lower() + '.csv')
            file['data'] = data
            file['columns'] = columns
            files.append(file)

        for file in files:
            self.output.data_to_csv(path=file['path'], data=file['data'], columns=file['columns'])


if __name__ == '__main__':
    schedule = Schedule()
    schedule.main()
