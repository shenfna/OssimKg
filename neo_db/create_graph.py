from neo4j import GraphDatabase
from Config import Conf


class CqlCreator:
    @staticmethod
    def update_node_cql(tx, node, primary_keys):
        node_type = node['node_type']
        node_attrs = node['node_attrs']
        cql = "MERGE (n:" + node_type + " {"
        for key in primary_keys:
            cql = cql + key + ": $" + key + ", "
        cql = cql[:-2] + "}) set n += {"
        for key, val in node_attrs.items():
            cql = cql + key + ": $" + key + ", "
        cql = cql[:-2] + "} return n"
        # value = ("%s = %s" % (key, val) for key, val in node_attrs.items())
        print(cql)
        # print(node_attrs)
        tx.run(cql, node_attrs)

    @staticmethod
    def update_relation_cql(tx, relation):
        out_node = relation['out_node']
        in_node = relation['in_node']
        name = relation['relation_name']
        relation_attrs = relation['relation_attrs']
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
                cql = cql + key + ": $" + "out_" + key + ", "
            cql = cql[:-2] + "}), "
            cql = cql + "(i"
            if in_node_type:
                cql = cql + ":" + in_node_type
            cql = cql + " {"
            for key in in_node_attrs.keys():
                cql = cql + key + ": $" + "in_" + key + ", "
            cql = cql[:-2] + "})"
            cql = cql + " MERGE (o)-[r:" + name + "]->(i) "
            if relation_attrs:
                cql += "SET r."
                for attr in relation_attrs:
                    cql = cql + attr + "=$" + "rel_" + attr + ", r."
                cql = cql[:-4]
            print(cql)
            # 实际参数
            attrs_merged = {}
            for key, val in out_node_attrs.items():
                attrs_merged["out_" + key] = val
            for key, val in in_node_attrs.items():
                attrs_merged["in_" + key] = val
            for key, val in relation_attrs.items():
                attrs_merged["rel_" + key] = val
            # print(attrs_merged)
            tx.run(cql, attrs_merged)

            # @classmethod
            # def delete_relation_cql_by_out_node(cls, tx, relation):
            #     orient_headers = relation['orient_headers']
            #     out_node = relation['out_node']
            #     in_node = relation['in_node']
            #     name = relation['relation_name']
            #     out_node_attrs = out_node['node_attrs']
            #     out_node_type = out_node['node_type']
            #     in_node_type = in_node['node_type']
            #     if out_node_attrs:
            #         cql = "MATCH (o"
            #         if out_node_type:
            #             cql = cql + ":" + out_node_type
            #         cql = cql + " {"
            #         for key in out_node_attrs.keys():
            #             cql = cql + orient_headers[key] + ": $" + key + ", "
            #         cql = cql[:-2]
            #         cql = cql + "})-[p:" + name + "]->"
            #         cql = cql + "(i"
            #         if in_node_type:
            #             cql = cql + ":" + in_node_type
            #         cql = cql + ") DELETE p"
            #         print(cql)
            #         tx.run(cql, out_node_attrs)
            #
            # @classmethod
            # def read_relation_cql_by_name(cls, tx, relation):
            #     orient_headers = relation['orient_headers']
            #     out_node = relation['out_node']
            #     in_node = relation['in_node']
            #     name = relation['relation_name']
            #     out_node_attrs = out_node['node_attrs']
            #     out_node_type = out_node['node_type']
            #     in_node_attrs = in_node['node_attrs']
            #     in_node_type = in_node['node_type']
            #     cql = "MATCH (o"
            #     if out_node_type:
            #         cql = cql + ":" + out_node_type
            #     cql = cql + " {"
            #     for key in out_node_attrs.keys():
            #         cql = cql + orient_headers[key] + ": $" + key + ", "
            #     cql = cql[:-2] + "})"
            #     cql = cql + "-[p:" + name + "]->(i"
            #     if in_node_type:
            #         cql = cql + ":" + in_node_type
            #     cql = cql + " {"
            #     for key in in_node_attrs.keys():
            #         cql = cql + orient_headers[key] + ": $" + key + ", "
            #     cql = cql[:-2] + "})"
            #     cql = cql + " RETURN p"
            #     print(cql)
            #     attrs_merged = in_node_attrs.copy()
            #     attrs_merged.update(out_node_attrs)
            #     print(attrs_merged)
            #     results = tx.run(cql, attrs_merged)
            #     return results


class Importer:
    def __init__(self):
        self.conf = Conf(file='neo4j.cfg')
        self.uri = self.conf.get('outputNeoDB', 'uri')
        self.user = self.conf.get('outputNeoDB', 'user')
        self.password = self.conf.get('outputNeoDB', 'password')
        self.driver = GraphDatabase.driver(uri=self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    # def insert_node(self, nodes):
    #     with self.driver.session() as session:
    #         for node in nodes:
    #             session.write_transaction(CqlCreator.create_node_cql, node)


    def update_node(self, nodes, primary_keys):
        with self.driver.session() as session:
            for node in nodes:
                session.write_transaction(CqlCreator.update_node_cql, node, primary_keys)

    # def update_node_property(self, nodes, primary_keys):
    #     with self.driver.session() as session:
    #         for node in nodes:
    #             session.write_transaction(CqlCreator.update_node_cql, node, primary_keys)

    def update_relation(self, relations):
        with self.driver.session() as session:
            for rel in relations:
                print(rel)
                session.write_transaction(CqlCreator.update_relation_cql, rel)


if __name__ == '__main__':
    cre = CqlCreator()
    in_node = {'node_type': 'Person', 'node_attrs': {'name': 'Alice', 'id': '1'}}
    out_node = {'node_type': 'Person', 'node_attrs': {'name': 'Mike', 'id': '2'}}
    relation = {'in_node': in_node, 'out_node': out_node, 'relation_name': 'KNOWS', 'relation_attrs': {'test_attr' : '1','test5_attrs':'5'}}
    cre.update_relation_cql(tx='', relation=relation)
    # it = Importer()
    # cre.update_node_cql(tx='', node={'node_type': 'Host', 'node_attrs': {'hostname': 'alienvault'}},
    #                    primary_keys=['hostId'])
