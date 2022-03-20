from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import configHandler as cfg
import pandas as pd
import json
import uuid

class cassandraOps:

    def __init__(self,clg):
        self.clg = clg
        self.isConnected = self.connectDataStax()

    def connectDataStax(self):
        '''
        :return: It will return session if db connection is successful else will return False
        '''
        try:
            ch      = cfg.configHandler("config.ini")
            options = ch.readConfigSection("cassandra")

            bundle          = options['bundle']
            client_id       = options['client_id']
            client_secret   = options['client_secret']
            self.key_space  = options['key_space']

            cloud_config = {
                'secure_connect_bundle': bundle
            }
            auth_provider = PlainTextAuthProvider(client_id,client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = cluster.connect()
            self.useOrCreateKeySpace()

            msg = "Connected cassandra db with cluster - {} and keyspace - {}".format(str(cluster),self.key_space)
            self.clg.log(msg)

        except Exception as e:
            msg = "Couldn't connect cassandra db server Getting error " + str(e)
            self.clg.log(msg,"ERROR")
            return False
        else:
            return True

    def useOrCreateKeySpace(self):
        """
        Function will create keyspace if not present
        :param session: session required to connect cassandra
        :param keyspace: keyspace to search
        :return:
        """
        try:
            self.clg.log("(useOrCreateKeySpace) Here for keyspace" +  self.key_space)
            searchQry = "SELECT * FROM system_schema.keyspaces WHERE keyspace_name='" +  self.key_space + "'"
            result = self.session.execute(searchQry).one()

            if (result):
                self.clg.log("(useOrCreateKeySpace) keyspace already exists")
            else:
                self.clg.log("(useOrCreateKeySpace) creating new keyspace")
                createQry = "CREATE KEYSPACE "+ self.key_space+" WITH replication={'class':'SimpleStrategy','replication_factor':'1'} AND durable_writes='true';"
                self.session.execute(createQry).one()

            qry = "USE " +  self.key_space
            self.clg.log("Query " + qry)
            self.session.execute(qry).one()
            self.clg.log("(useOrCreateKeySpace) using keyspace " +  self.key_space)
        except Exception as e:
            raise Exception(f"(useOrCreateKeySpace) Couldn't use keyspace\n" + str(e))

    def isTablePresent(self,table_name):
        try:
            table_name = table_name.replace("-","_")

            if (self.isConnected):
                searchQry = "SELECT table_name FROM system_schema.tables WHERE keyspace_name='"+  self.key_space + "' AND table_name = '"+ table_name + "'"
                result = self.session.execute(searchQry).one()

                if(result is None):
                    msg = "(isTablePresent) table " + table_name + " doesn't exist"
                    self.clg.log(msg)
                    return False

                self.clg.log("(isTablePresent) result - " + str(result))
                if (result[0] == table_name):
                    msg = "(isTablePresent) table " + table_name + " exist"
                    self.clg.log(msg)
                    return True
                else:
                    msg = "(isTablePresent) table " + table_name + " doesn't exist"
                    self.clg.log(msg)
                    return False
            else:
                msg = "(isTablePresent) DB not connected "
                self.clg.log(msg)
                return False

        except Exception as e:
            msg = "(isTablePresent) Getting error " + str(e)
            self.clg.log(msg, "ERROR")
            return False

    def findAllRecords(self,table_name):
        """
        find all records in DB for passed table
        :param table_name: table name of DB
        :return: cassandra result set
        """
        try:
            table_name = table_name.replace("-", "_")

            if(self.isTablePresent(table_name)):
                query = "SELECT * FROM " + table_name
                all_Records = self.session.execute(query)
                msg = "(findAllRecords) Fetched data from table {}".format(table_name)
                self.clg.log(msg)
                return all_Records
            else:
                msg = "(findAllRecords) table {} does not exists".format(table_name)
                self.clg.log(msg)
                # raise Exception(msg)
                return False

        except Exception as e:
            msg = "(findAllRecords) Couldn't fetch data from provided table " + str(e)
            self.clg.log(msg, "ERROR")
            raise Exception(msg)

    def findRecordWhere(self, table_name, top=0,where="",orderby ="",sort='ASC', byPrimary=False):
        """
        To fire query based on inputs to find records in table
        :param table_name: table of DB
        :param top: Limit of records
        :param where: where clause with column=value pair AND conditions in string
        :param orderby: String of Columns separated by comma
        :param sort: Default is "ASC" also can be "DESC"
        :param byPrimary: True if search is only using Primary key
        :return: returns result set if found else -1
        """

        try:
            table_name = table_name.replace("-", "_")

            if(self.isTablePresent(table_name)):

                query = "SELECT * FROM " + table_name

                if where is not None and where.strip() != "":
                    query = query + " WHERE " + where

                if orderby is not None and orderby.strip() != "":
                    query = query + " ORDER BY " + orderby + sort

                if (top > 1):
                    query = query + " LIMIT " + top

                if(byPrimary == False):
                    self.clg.log("(findRecordWhere) Where condition has non Primary column")
                    firstColumn = where.split("=")[0]
                    self.clg.log("(findRecordWhere) first Column in where"+firstColumn)
                    if(self.createIndexOn(table_name,firstColumn) == False):
                        query = query + " ALLOW FILTERING"

                msg = "(findRecordWhere) Search data query {}".format(query)
                result = self.session.execute(query).one()

                self.clg.log(msg)
                return result
            else:
                msg = "(findRecordWhere) Provided table {} doesn't exists ".format(table_name)
                self.clg.log(msg, "ERROR")
                return -1
        except Exception as e:
            msg = "(findRecordWhere) Couldn't fetch data from provided table " + str(e)
            self.clg.log(msg, "ERROR")
            raise Exception(msg)


    def getDataFrameFromTable(self, table_name):
        """
        This function returns table records in the form of data frame
        :param table_name: db table name
        :return: data frame of all records present under table
        """
        try:
            table_name = table_name.replace("-", "_")

            all_Records = self.findAllRecords(table_name)
            if(all_Records != False and len(all_Records.column_names) > 0):
                dataframe = pd.DataFrame(all_Records)
                msg = "(getDataFrameFromTable) Converted result set of table {} to data frame".format(table_name)
                self.clg.log(msg)
                return dataframe
        except Exception as e:
            msg = "(getDataFrameFromTable) Couldn't convert result set to data frame " + str(e)
            self.clg.log(msg,"ERROR")
            raise Exception(msg)

    def getListOfAllRecords(self, table_name):
        """
        This function returns table records in the form of list
        :param table_name: table of DB
        :return: result list
        """
        try:
            table_name = table_name.replace("-", "_")

            all_Records = self.findAllRecords(table_name)
            if (all_Records != False and len(all_Records.column_names) > 0):
                msg = "(getListOfAllRecords) Returning result list of table {} for display ".format(table_name)
                self.clg.log(msg)
                return [i for i in all_Records]
            return {"Err": "No Records found"}
        except Exception as e:
            msg = "(getListOfAllRecords) Couldn't fetch data from provided table for diaplay " + str(e)
            self.clg.log(msg, "ERROR")
            raise Exception(msg)

    def createTable(self,table_name,dataDict={},checkDType=False,primarykey="",primarykeyType="UUID"):
        """
        Function to create table
        :param table_name: name of table to create
        :param dataDict: data set with column and corresponding values
        :param checkDType: must be True if column data type must be decided based on values passed against them
        :param primarykey: primary key column name if required any
        :return:
        """
        try:
            table_name = table_name.replace("-", "_")

            if(self.isConnected == False):
                msg = "(createTable) DB is not connected"
                self.clg.log(msg, "ERROR")
                return False

            columns_lst = list(dataDict.keys())
            count = len(columns_lst)
            if(count > 0 ):
                createQry = "CREATE TABLE "+table_name\

                if(primarykey is not None and primarykey.strip() != ""):
                    createQry =  createQry  + " (" + str(primarykey) + " "+ primarykeyType +" PRIMARY KEY,"
                else :
                    createQry = createQry + " (id UUID PRIMARY KEY," #Primary key By default

                for column in columns_lst:

                    if(primarykey is not None and primarykey.strip() != "" and str(column) == str(primarykey)):
                        self.clg.log("In create query Already added primary key {}".format(primarykey))
                        if (column == columns_lst[count - 1]):
                            createQry = createQry + ") "

                    else:
                        if(checkDType):
                            if(type(dataDict[column]) == int):
                                createQry = createQry + str(column) + " VARINT"
                            elif(type(dataDict[column]) == float):
                                createQry = createQry + str(column) + " DOUBLE"
                            elif (type(dataDict[column]) == list):
                                createQry = createQry + str(column) + " LIST<TEXT>"
                            else:
                                createQry = createQry + str(column) + " TEXT"
                        else:
                            createQry = createQry + str(column) + " TEXT"

                        if (column == columns_lst[count - 1]):
                            createQry = createQry + ") "
                        else:
                            createQry = createQry + ", "

                self.clg.log("Create Query {}".format(createQry))
                result = self.session.execute(createQry).one()
                # print(result)
                self.clg.log('(createTable) result- '+ str(result))

            if(result):
                msg = "(createTable) Created table {}".format(table_name)
                self.clg.log(msg)
                return True
            else:
                msg = "(createTable) Couldn't create table {}".format(table_name)
                self.clg.log(msg, "ERROR")
                return False

        except Exception as e:
            msg = "(createTable) Couldn't create table " + str(e)
            self.clg.log(msg, "ERROR")
            raise Exception(msg)

    def insertRecord(self,table_name,insJson,bDefualtPrimary=True):
        try:
            table_name = table_name.replace("-", "_")

            if (self.isConnected == False):
                msg = "(insertRecord) DB is not connected"
                self.clg.log(msg, "ERROR")
                return False

            self.clg.log('insJson -> '+str(insJson))
            count  = len(insJson)
            if (count > 0 and self.isTablePresent(table_name)):
                insQry = "INSERT INTO "+ table_name + "("

                if(bDefualtPrimary):
                    insQry = insQry + "id,"

                # Loop for column name
                i = 1
                for keys in list(insJson.keys()):
                    if (i == count):
                        insQry = insQry + str(keys) + ") VALUES ("
                    else:
                        insQry = insQry + str(keys) + ","
                    i +=1

                if (bDefualtPrimary):
                    insQry = insQry + "uuid(),"

                # Loop for column values
                i = 1
                for keys in list(insJson.keys()):
                    data = insJson[keys]
                    print('data -> '+str(data))

                    if(type(data) == list and len(data) > 0):
                        insQry = insQry + "{}".format(data)
                    elif(type(data) == int or type(data) == float):
                        insQry = insQry + "{}".format(data)
                    elif data is not None and len(str(data)) > 0 and str(data).strip() != "":
                        insQry = insQry + "'" + str(data) + "'"
                    else:
                        insQry = insQry + "null"

                    if (i == count):
                        insQry = insQry + ")"
                    else:
                        insQry = insQry + ","
                    i += 1

                print("insQuery ->"+insQry)
                self.clg.log("(insertRecord) Insert Query {}".format(insQry))
                result = self.session.execute(insQry).one()
                # print(result)
                self.clg.log('(insertRecord) result- ' + str(result))

                msg = "(insertRecord) Inserted record in table {}".format(table_name)
                self.clg.log(msg)
                return True
            else:
                msg = "(insertRecord) Either table is not present or empty dictionary passed for insertion"
                self.clg.log(msg, "ERROR")
                return False

        except Exception as e:
            msg = "(insertRecord) Couldn't insert data in table {} Getting error - {} ".format(table_name,str(e))
            self.clg.log(msg, "ERROR")
            raise Exception(msg)


    def insertJSON(self, table_name, insJson, bDefualtPrimary=True):
        try:
            table_name = table_name.replace("-", "_")

            if (self.isConnected == False):
                msg = "(insertJSON) DB is not connected"
                self.clg.log(msg, "ERROR")
                return False

            count = len(insJson)
            if (count > 0 and self.isTablePresent(table_name)):

                if (bDefualtPrimary):
                    insJson['id'] = str(uuid.uuid1())

                sInsJSON = str(insJson).replace("'", "\"")
                self.clg.log('insJson string -> ' + str(insJson))
                # print("sInsJSON-> ->" + sInsJSON)

                insQry = 'INSERT INTO '+ table_name +' JSON \'' + sInsJSON + '\''


                self.clg.log("(insertJSON) Insert Query {}".format(insQry))
                result = self.session.execute(insQry).one()
                # print(result)
                self.clg.log('(insertJSON) result- ' + str(result))

                msg = "(insertJSON) Inserted record in table {}".format(table_name)
                self.clg.log(msg)
                return True
            else:
                msg = "(insertJSON) Either table is not present or empty dictionary passed for insertion"
                self.clg.log(msg, "ERROR")
                return False

        except Exception as e:
            msg = "(insertJSON) Couldn't insert data in table {} Getting error - {} ".format(table_name, str(e))
            self.clg.log(msg, "ERROR")
            raise Exception(msg)

    def saveDictDataIntoTable(self, table_name, data_dict):
        """
        function will save dictionary  data into table
        :param table_name: db table name
        :param data_dict: dictionary with key as column and their values
        :return: Will return True if data saved successfully else will return False
        """
        try:
            table_name = table_name.replace("-", "_")

            if (self.isConnected == False):
                msg = "(saveDictDataIntoTable) DB is not connected"
                self.clg.log(msg, "ERROR")
                return False

            if(self.isTablePresent(table_name)):
                # self.insertRecord(table_name,data_dict)
                self.insertJSON(table_name,data_dict)
            else:
                if(self.createTable(table_name, data_dict, checkDType=True)):
                    # self.insertRecord(table_name, data_dict)
                    self.insertJSON(table_name,data_dict)

            msg = "(saveDictDataIntoTable) Saved dictionary data to provided table {}".format(table_name)
            self.clg.log(msg)
            return True
        except Exception as e:
            msg = "(saveDictDataIntoTable) Couldn't save dictionary data to provided table " + str(e)
            self.clg.log(msg, "ERROR")
            raise Exception(msg)

    def saveDataFrameIntoTable(self, table_name, dataframe):
        """
        function will save data frame data into table
        :param table_name: db table name
        :param dataframe: data frame
        :return: Will return True if data saved successfully else will return False
        """
        try:
            table_name = table_name.replace("-", "_")

            if (self.isConnected == False):
                msg = "(saveDataFrameIntoTable) DB is not connected"
                self.clg.log(msg, "ERROR")
                return False

            dataframe_dict = json.loads(dataframe.T.to_json())

            if (self.isTablePresent(table_name)):
                # self.insertRecord(table_name, dataframe_dict)
                self.insertJSON(table_name, dataframe_dict)
            else:
                if (self.createTable(table_name, dataframe_dict, checkDType=True)):
                    # self.insertRecord(table_name, dataframe_dict)
                    self.insertJSON(table_name, dataframe_dict)

            msg = "(saveDataFrameIntoTable) Saved dataframe data to provided table {}".format(table_name)
            self.clg.log(msg)
            return True
        except Exception as e:
            msg = "(saveDataFrameIntoTable) Couldn't save dataframe data to provided table " + str(e)
            self.clg.log(msg, "ERROR")
            raise Exception(msg)


    def createIndexOn(self,table_name,column_name):
        try:
            table_name = table_name.replace("-", "_")

            if (self.isTablePresent(table_name)):

                index_name = table_name + "_by_" + column_name
                query = "CREATE INDEX IF NOT EXISTS {} ON {}({});".format(index_name,table_name,column_name)
                self.clg.log('query - ' + query)

                self.session.execute(query)
                msg = "(createIndexOn) Created Index Successfully"
                self.clg.log(msg)
                return True
            else:
                msg = "(createIndexOn) Either table is not present Or DB is not Connected"
                self.clg.log(msg, "ERROR")
                return False

        except Exception as e:
            msg = "(createIndexOn) Couldn't create index on table " + str(e)
            self.clg.log(msg, "ERROR")
            # raise Exception(msg)
            return False

    # For debugging purpose
    def fireQuery(self, query):
        try:
            print("query -> "+query)
            result = self.session.execute(query)
            return result
        except Exception as e:
            msg = "(createIndexOn) Couldn't fire query " + str(e)
            raise Exception(msg)


if(__name__ == "__main__"):
    import customLogger as lgr
    clg = lgr.customLogger(__name__)
    db = cassandraOps(clg)
    print(db.isConnected)
    # df = db.getDataFrameOfCollection("emp")
    # print(df)
    # result = db.getListOfAllRecords("iphone7")
    # whereQry = "product_name= 'APPLE iPhone 7 (Silver, 32 GB)'"
    # result = db.findRecordWhere(table_name="iphone7", top=1, where=whereQry)
    # query = "SELECT * FROM iphone7 WHERE product_name='APPLE' ALLOW FILTERING"
    query = "SELECT * FROM readme_note7"
    #query = "DROP TABLE redmi7"
    # query = 'INSERT INTO iphone7 JSON \'{"product_name": "APPLE iPhone 7 (Silver, 32 GB)", "product_searched": "iphone7", "price": "₹24,999", "offer_details": [], "discount_percent": "20% off", "EMI": "NO EMI Plans", "rating": "5", "comment": "Worth every penny", "customer_name": "Hemanta Sa", "review_age": "Sep, 2019", "id": "b5513b52-fdba-11eb-b9a2-8c554a9ad683"}\''
    result = db.fireQuery(query)
    print(result)
    for i in result:
        print(i)

    # db.isTablePresent('redmi8')

    # import uuid
    # print(str(uuid.uuid1()))
