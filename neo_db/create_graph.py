from neo4j import GraphDatabase
from Config import Conf


class CqlCreator:

    @staticmethod
    def create_node_sql(tx, node):
        orient_headers = node['orient_headers']
        node_type = node['node_type']
        node_attrs = node['node_attrs']
        cql = "MERGE (n:" + node_type + " {"
        for key, val in node_attrs.items():
            cql = cql + key + ": $" + orient_headers[key] + ", "
        cql = cql[:-2] + "})"
        # value = ("%s = %s" % (key, val) for key, val in node_attrs.items())
        print(cql)
        print(node_attrs)
        tx.run(cql, node_attrs)

    @staticmethod
    def update_node_sql(tx, node, condition):
        orient_headers = node['orient_headers']
        node_type = node['node_type']
        if node_type:
            cql = "MATCH (n: " + node_type + ") WHERE "
        else:
            cql = "MATCH (n) WHERE"
        for key, val in condition.items():
            cql = cql + "n." + orient_headers[key] + " ='" + str(val) + "' AND "
        cql = cql[:-5]
        node_attrs = node['node_attrs']
        if node_attrs:
            cql = cql + " SET "
            for key, val in node_attrs.items():
                cql = cql + "n." + orient_headers[key] + " ='" + str(val) + "' , "
            cql = cql[:-3]
        cql = cql + " RETURN n"
        print(cql)
        results = tx.run(cql, node_attrs)
        # for result in results:
        #     print((result['n']).__dict__['_properties']['id'])
        return results

    @staticmethod
    def create_relation_sql(tx, relation):
        orient_headers = relation['orient_headers']
        out_node = relation['out_node']
        in_node = relation['in_node']
        name = relation['relation_name']
        out_node_attrs = out_node['node_attrs']
        out_node_type = out_node['node_type']
        in_node_attrs = in_node['node_attrs']
        in_node_type = in_node['node_type']
        if out_node_attrs and in_node_attrs:
            cql = "MATCH (o"
            if out_node_type:
                cql = cql + ":" + out_node_type
            cql = cql + " {"
            for key in out_node_attrs.keys():
                cql = cql + orient_headers[key] + ": $" + key + ", "
            cql = cql[:-2] + "})"
            cql = cql + " MATCH (i"
            if in_node_type:
                cql = cql + ":" + in_node_type
            cql = cql + " {"
            for key in in_node_attrs.keys():
                cql = cql + orient_headers[key] + ": $" + key + ", "
            cql = cql[:-2] + "})"
            cql = cql + " MERGE (o)-[:" + name + "]->(i)"
            print(cql)
            attrsMerged = in_node_attrs.copy()
            attrsMerged.update(out_node_attrs)
            print(attrsMerged)
            tx.run(cql, attrsMerged)


class Importer:
    def __init__(self):
        self.conf = Conf(file='neo4j.cfg')
        self.uri = self.conf.get('outputNeoDB', 'uri')
        self.user = self.conf.get('outputNeoDB', 'user')
        self.password = self.conf.get('outputNeoDB', 'password')
        self.driver = GraphDatabase.driver(uri=self.uri, auth=(self.user, self.password))
        self.cql_creator = CqlCreator()

    def close(self):
        self.driver.close()

    def insert_node(self, nodes):
        with self.driver.session() as session:
            for node in nodes:
                session.write_transaction(self.cql_creator.create_node_sql, node)

    def update_node(self, nodes):
        with self.driver.session() as session:
            for node in nodes:
                # print(node['node_type'])
                node_none = {
                    'node_type': node['node_type'],
                    'node_attrs': '',
                    'orient_headers': node['orient_headers']
                }
                condition = node['condition']
                result = session.read_transaction(self.cql_creator.update_node_sql, node_none, condition)
                if len(list(result)) != 0:
                    result = session.write_transaction(self.cql_creator.update_node_sql, node, condition)
                    # print(len(list(result)))
                else:
                    # self.insert_node(node)
                    session.write_transaction(self.cql_creator.create_node_sql, node)

    def insert_relation(self, relations):
        with self.driver.session() as session:
            for rel in relations:
                session.write_transaction(self.cql_creator.create_relation_sql, rel)


if __name__ == '__main__':
    cre = CqlCreator()
    in_node = {'node_type': 'Person', 'node_attrs': {'name': 'Alice', 'id': '1'}}
    out_node = {'node_type': 'Person', 'node_attrs': {'name': 'Mike', 'id': '2'}}
    relation = {'in_node': in_node, 'out_node': out_node, 'relation_name': 'KNOWS'}
    cre.create_relation_sql(tx='', relation=relation)
    # it = Importer()
    # it.update_node_sql(node={'node_type': 'Host', 'node_attrs': {'hostname': 'alienvault'}},
    #                    conditions={'id': '3B3E81B95A9211E99A5D9088F9497FA1'})
