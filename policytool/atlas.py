import requests

class Client:

    def __init__(self, url_prefix, auth=None):
        """
        :param url_prefix: Prefix of the URL to the Atlas API. Example: 'http://atlas.host:21000/api/atlas'
        :param auth: If authentication is used. For Kerberos HTTPKerberosAuth(principal="user@MY.REALM")
        """
        self.url_prefix = url_prefix # http://atlas.host.my.org:21000/api/atlas/
        self.auth=auth


    def _search(self, query):
        return requests.post(self.url_prefix + "/v2/search/basic", json=query, auth=self.auth)


    def _create_qualifiedname_query(self, type_name, *values):
        """
        See _get_qualified_name for reason why implemented like this.
        :param type: type of expected entities
        :param values: Provide as many as you know of schema, table, column in that order.
        :return: Query to be sent to Atlas API.
        """
        query = {}
        query['typeName']=type_name
        query['excludeDeletedEntities']=True
        query['limit']=10000
        entityFilter={}
        entityFilter['condition']='AND'
        criterion=[]
        n=0
        for v in values:
            criteria={}
            criteria['attributeName'] = 'qualifiedName'
            criteria['operator'] = 'STARTSWITH' if n==0 else 'CONTAINS'
            criteria['attributeValue'] = v
            n+=1
            criterion.append(criteria)
        entityFilter['criterion']=criterion
        query['entityFilters']=entityFilter
        return query


    def _filter_entities_on_qualifiedName(self, entities, qualtifiedName):
        return [e for e in entities if e['attributes']['qualifiedName'].startswith(qualtifiedName)]


    def _create_qualifiedName_prefix(self, *values):
        return ".".join(values)+"."

    def _get_qualified_name(self, type, *values):
        """
        This method together with _create_qualifiedname_query are a bit weird implemented.
        Reason for not searching on the starts_with of the qualified name direct in Atlas API is that the
        search is shaky when searching on string with dot or underscore (not fully investigated). For instance
        searching on "hadoop_out_utv.vegas_title_d." gives a server error (500) but "foo.product_s." works fine.
        Therefore we do an "AND" search of all desired substrings of the qualified name, which gives us a super-set
        of our wanted result, but it is of a reasonable size. Then we do the final fully correct filtering on the
        client side.

        :param type: type of wanted entities
        :param values: Provide as many as you know of schema, table, column in that order.
        :return: Array of one dict per entity. Dict is on form:
            {u'status': u'ACTIVE',
             u'guid': u'1bbe630c-927e-43f5-846b-94513db1d625',
             u'typeName': u'hive_table',
             u'displayText': u'tablename',
             u'attributes': {
                u'owner': u'owner',
                u'qualifiedName': u'database.tablename@dhadoopname',
                u'name': u'tablename',
                u'description': None},
             u'classificationNames': [u'TAG1', u'TAG2']}
        """
        query = self._create_qualifiedname_query(type, *values)
        response = self._search(query)
        if response.status_code == 200:
            json_resopnse = response.json()
            if json_resopnse.has_key('entities'):
                return self._filter_entities_on_qualifiedName(json_resopnse['entities'], self._create_qualifiedName_prefix(*values))
            else:
                return []
        else:
            raise AtlasError(response.content, response.status_code)

    def get_tables(self, db):
        """
        Get all active tables in a hive database.
        :param db: Name of database to get all tables for.
        :return: Array of one dict per table. Dict is on form:
            {u'status': u'ACTIVE',
             u'guid': u'1bbe630c-927e-43f5-846b-94513db1d625',
             u'typeName': u'hive_table',
             u'displayText': u'tablename',
             u'attributes': {
                u'owner': u'owner',
                u'qualifiedName': u'database.tablename@hadoopname',
                u'name': u'tablename',
                u'description': None},
             u'classificationNames': [u'TAG1', u'TAG2']}
        """
        return self._get_qualified_name("hive_table", db)


    def get_columns(self, db, table):
        """
        Get all columns for a table.
        :param db: Name of database
        :param table: Name of table
        :return: Array of one dict per column. Dict is on form:
            {u'status': u'ACTIVE',
             u'guid': u'7880d2a3-fec5-4b35-a91b-bea6c75f56b1',
             u'typeName': u'hive_column',
             u'displayText': u'columnname',
             u'attributes': {
                u'owner': u'user',
                u'qualifiedName': u'database.table.columnname@hadoopname',
                u'name': u'columnname',
                u'description': None},
            u'classificationNames': [u'TAG1', u'TAG2']}

        """
        return self._get_qualified_name("hive_column", db, table)


    def add_tags_on_guid(self, guid, tags):
        """
        Add Tags to an entity.
        :param guid: guid on the entity(e.g. column or table) to tag.
        :param tags: Array of strings(tags) to add.
        """
        tags_struct = []
        for t in tags:
            tags_struct.append({"typeName": t})

        response = requests.post(self.url_prefix + "/v2/entity/guid/" + guid + "/classifications", json=tags_struct, auth=self.auth)
        if response.status_code != 204:
            raise AtlasError(response.content, response.status_code)


    def delete_tags_on_guid(self, guid, tags):
        """
        Add Tags to an entity.
        :param guid: guid on the entity(e.g. column or table) to tag.
        :param tags: Array of strings(tags) to remove.
        """
        failed_tags = []
        for t in tags:
            response = requests.delete(self.url_prefix + "/v2/entity/guid/" + guid + "/classification/" + t, auth=self.auth)
            if response.status_code != 204:
                failed_tags.append(t)
        if len(failed_tags) != 0:
            raise AtlasError("Failed to delete tags " + repr(tags) + " on " + guid)


    def known_tags(self):
        """
        Get all tags know by Atlas
        :return: Array of one dict per tag. Dict is on form:
            {u'category': u'CLASSIFICATION', u'guid': u'5a76bab9-02ec-434d-bbee-1c7294f0cf31', u'name': u'PII'}
        """
        response = requests.get(self.url_prefix + "/v2/types/typedefs/headers", auth=self.auth)
        if response.status_code == 200:
            return [e for e in response.json() if e['category']=='CLASSIFICATION']
        else:
            raise AtlasError(response.content, response.status_code)

    def add_tag_definitions(self, tags):
        """
        Create new tag definitions in Atlas.
        :param tags: Array of strings that are new tags. Tags must not exist in Atlas. Use known_tags() to figure out.
        :return: None
        """
        post_data={"classificationDefs": list([{"name": t, "description":"", "superTypes":[], "attributeDefs":[]} for t in tags])}
        response=requests.post(self.url_prefix + "/v2/types/typedefs?type=classification", auth=self.auth, json=post_data)
        if response.status_code != 200:
            raise AtlasError(response.content, response.status_code)



class AtlasError(Exception):
    def __init__(self, message, http_code=None):
        self.message = message
        self.http_code = http_code
    def __str__(self):
        return "HTTP code: " + repr(self.http_code) + " Message: " + repr(self.message)
