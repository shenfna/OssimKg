from neo4j import GraphDatabase
from Config import Conf


class Importer:
    def __init__(self):
        self.conf = Conf(file='neo4j.cfg')
        self.uri = self.conf.get('outputNeoDB', 'uri')
        self.user = self.conf.get('outputNeoDB', 'user')
        self.password = self.conf.get('outputNeoDB', 'passport')
        self.driver = GraphDatabase.driver(uri=self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    @staticmethod
    def create_node_sql(tx, node):
        node_type = node['node_type']
        node_attrs = node['node_attrs']
        cql = "MERGE (n:" + node_type + " {"
        for key, val in node_attrs.items():
            cql = cql + key + ": $" + key + ", "
        cql = cql[:-2] + "})"
        # value = ("%s = %s" % (key, val) for key, val in node_attrs.items())
        print(cql)
        print(node_attrs)
        tx.run(cql, node_attrs)

    @staticmethod
    def update_node_sql(tx, node, conditions):
        node_type = node['node_type']
        if node_type:
            cql = "MATCH (n: " + node_type + ") WHERE "
        else:
            cql = "MATCH (n) WHERE"
        for key, val in conditions.items():
            cql = cql + "n." + key + " ='" + str(val) + "' AND "
        cql = cql[:-5]
        node_attrs = node['node_attrs']
        if node_attrs:
            cql = cql + " SET "
            for key, val in node_attrs.items():
                cql = cql + "n." + key + " ='" + str(val) + "' , "
            cql = cql[:-3]
        cql = cql + " RETURN n"
        print(cql)
        results = tx.run(cql)
        # for result in results:
        #     print((result['n']).__dict__['_properties']['id'])
        return results

    def insert_node(self, node):
        with self.driver.session() as session:
            session.write_transaction(self.create_node_sql, node)

    def update_node(self, node, conditions):
        node_none = {
            'node_type': node['node_type'],
            'node_attrs': ''
        }
        with self.driver.session() as session:
            result = session.read_transaction(self.update_node_sql, node_none, conditions)
            if len(list(result)) != 0:
                result = session.read_transaction(self.update_node_sql, node, conditions)
                print(list(result))
            else:
                self.insert_node(node)


if __name__ == '__main__':
    it = Importer()
    # it.update_node_sql(node={'node_type': 'Host', 'node_attrs': {'hostname': 'alienvault'}},
    #                    conditions={'id': '3B3E81B95A9211E99A5D9088F9497FA1'})
