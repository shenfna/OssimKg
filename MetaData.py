import xml.dom.minidom
from PathUtils import get_file_name

FILENAME = 'Mappings.xml'


class Extractor:

    @classmethod
    def get_meta_data(cls):
        dom = xml.dom.minidom.parse(get_file_name(file=FILENAME))
        root = dom.documentElement
        nodes = root.getElementsByTagName('mapping')

        mappings = dict()
        for node in nodes:
            name_node = node.getElementsByTagName('name')[0]
            columns_node = node.getElementsByTagName('columns')[0]
            source_node = node.getElementsByTagName('source')[0]
            mapping = dict()
            mapping_name = name_node.childNodes[0].data
            columns = []
            for column_node in columns_node.getElementsByTagName('column'):
                columns.append(column_node.childNodes[0].data)
            mapping['columns'] = columns
            mapping['source'] = source_node.childNodes[0].data
            mappings[mapping_name] = mapping

        return mappings


if __name__ == '__main__':
    test = Extractor()
    print(test.get_meta_data())
    # test.get_from_mysql()
