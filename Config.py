from configparser import ConfigParser

DEFAULT_CONFIG_FILE = 'mysql.cfg'


class Conf:
    def __init__(self, file=DEFAULT_CONFIG_FILE):
        self.Conf = ConfigParser()
        self.read(file=file)

    def read(self, file):
        try:
            self.Conf.read(file)
        except Exception as e:
            print(e)

    def get(self, section, option):
        try:
            value = self.Conf.get(section, option)
        except Exception as e:
            print(e)
            value = ""

        return value

    def get_int(self, section, option):
        try:
            value = self.Conf.getint(section, option)
        except Exception as e:
            print(e)
            value = ""

        return value


if __name__ == '__main__':
    conf = Conf()
    print(conf.get('db', 'db_port'))

