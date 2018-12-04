import re

from pyhive import hive


class Client:

    _conn = None

    def __init__(self, host, port=10000, auth="KERBEROS", service_name="hive", version=1):
        """
        :param host: Name of hive server.
        :param port: Thrift port of hiveserver
        :param auth: Authentication method, only kerberos supported for now.
        :param service_name: Kerberos service name. Defaults to hive.
        :param version: Version of hive.
        """
        self.host = host
        self.port = int(port)
        self.auth = auth
        self.service_name = service_name
        self.version = version

    def _connection(self):
        if not self._conn:
            self._conn = hive.Connection(
                host=self.host, port=self.port, auth=self.auth, kerberos_service_name=self.service_name)
        return self._conn

    @classmethod
    def _verify_entity_name(cls, entity):
        """
        To avoid SQL injection. Throws an exception if entity includes characters other than letters(a-z), numbers
        and underscore(_).
        """
        if re.search("[^a-zA-Z0-9_]", entity):
            raise HiveError("\"{}\" includes non allowed characters".format(entity))

    def get_location(self, database, table=None):
        Client._verify_entity_name(database)
        if table is not None and table != '*':
            Client._verify_entity_name(table)
            cursor = self._connection().cursor()
            cursor.execute("describe formatted {}.{}".format(database, table))
            for key, value, _ in cursor.fetchall():
                if key is not None and key.strip() == u'Location:':
                    return value.strip()
            # If we not find 'Location:', its probably a view.
            return None
        else:
            cursor = self._connection().cursor()
            cursor.execute("describe database {}".format(database))
            for _, _, location, _ , _, _ in cursor.fetchall():
                return location
        raise HiveError("Can not find location for {}.{}.".format(database, table))


class HiveError(Exception):
    def __init__(self, message, source_exception=None):
        self.message = message
        self.source_exception = source_exception

    def __str__(self):
        if self.source_exception:
            return "Source: " + repr(self.source_exception) + " Message: " + repr(self.message)
        else:
            return "Message: " + repr(self.message)
