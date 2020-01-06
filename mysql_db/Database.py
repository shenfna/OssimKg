from DBUtils import PooledDB
import pymysql
from Config import Conf


class DatabaseConn:
    DEFAULT_SQL = 'select hex(host_id), name from host_types left join device_types on host_types.subtype = device_types.id;'

    def __init__(self):
        # 构造函数，创建数据库连接、游标

        self.__pool = None
        self.config = Conf(file='mysql.cfg')
        self.__conn = self.connect()

    def connect(self):

        if self.__pool is None:
            self.__pool = PooledDB.PooledDB(creator=pymysql, mincached=1, maxcached=20,
                                            host=self.config.get(section='db', option='db_host'),
                                            user=self.config.get('db', 'db_user'),
                                            passwd=self.config.get('db', 'db_pass'),
                                            db=self.config.get('db', 'db_name'),
                                            port=self.config.get_int('db', 'db_port'),
                                            charset=self.config.get('db', 'db_charset'))
            print(self.__pool)
        return self.__pool.connection()

    def exec_query(self, query):
        """ 查询 """
        # print(query)
        try:
            cursor = self.__conn.cursor(cursor=pymysql.cursors.DictCursor)

        except Exception as e:
            print(e)
            return []

        cursor.execute(query)  # 执行sql
        select_res = cursor.fetchall()  # 返回结果为字典
        # for row in select_res:
            # print(row)
        # print(select_res)
        # print(cursor.rowcount)
        cursor.close()
        return select_res

    def close(self):

        if self.__conn is not None:
            self.__conn.close()


if __name__ == '__main__':

    # 申请资源
    opm = DatabaseConn()

    sql = opm.DEFAULT_SQL
    res = opm.exec_query(sql)

    # 释放资源
    opm.close()
