import configparser as cp

class configHandler:

    def __init__(self,filename):
        self.filename = filename

    def __str__(self):
        return self.filename

    def generateConfigFile(self):
        try:
            # This is a hardcoded schema with default values
            write_config = cp.ConfigParser()

            write_config.add_section("mongodb")
            write_config.set("mongodb", "DB_NAME", "dbname")

            write_config.add_section("cassandra")
            write_config.set("cassandra", "BUNDLE", "CassandraDataStraxPythonBundle\\secure-connect-dbname.zip")
            write_config.set("cassandra", "CLIENT_ID","dummy")
            write_config.set("cassandra", "CLIENT_SECRET", "dummy")
            write_config.set("cassandra", "KEY_SPACE", "keyspacename")

            write_config.add_section("log")
            write_config.set("log", "FILE_NAME", "Logs\\webscrapper.log")
            write_config.set("log", "FILE_NAME2", "Logs\\webscrapper_err.log")
            write_config.set("log", "LEVEL", "DEBUG")
            write_config.set("log", "LEVEL2", "ERROR")
            write_config.set("log", "MAX_BYTES", "1000000")
            write_config.set("log", "BACKUP_COUNT", "5")

            write_config.add_section("output")
            write_config.set("output", "directory", "Output")

            if('.ini' in self.filename):
                configFile = self.filename
            else:
                configFile = self.filename + ".ini"

            fw = open(configFile, 'w')
            write_config.write(fw)
            fw.close()
        except Exception as e:
            print('generateConfigFile Failed with exception - {}'.format(e))


    def readConfigSection(self,section):
        try:
            options = {}
            parser = cp.ConfigParser()

            if ('.ini' in self.filename):
                configFile = self.filename
            else:
                configFile = self.filename + ".ini"

            parser.read(configFile)

            if(parser.has_section(section)):
                for (each_key, each_val) in parser.items(section):
                    # print("key - {} , value - {}".format(each_key,each_val))
                    options[each_key] = each_val

            # if(section == "mongodb"):
            #     return parser.get(section,"dbserver")
            # elif(section == "mysql"):
            #     return parser.get(section,"dbserver"),parser.get(section, "user"),parser.get(section, "passwd")
            # elif (section == "cassandra"):
            #     return parser.get(section, "user"),parser.get(section, "passwd")
            # else:
            #     return -1
            if(len(options) > 0):
                return options
            else:
                return -1
        except Exception as e:
            # print('readConfigFileItem Failed with exception - {}'.format(e))
            return -1

    def readConfigOptions(self,section,option):

        try:

            parser = cp.ConfigParser()

            if ('.ini' in self.filename):
                configFile = self.filename
            else:
                configFile = self.filename + ".ini"

            parser.read(configFile)
            if(parser.has_option(section,option)):
                return parser.get(section, option)
            else:
                return -1
        except Exception as e:
            # print('readConfigFileItem Failed with exception - {}'.format(e))
            return -1



# if (__name__ == "__main__"):
#     ch = configHandler("config.ini")
#     # ch.generateConfigFile() # Uncomment this line to generate ini file
#
#     res = ch.readConfigOptions("cassandra","CLIENT_SECRET")
#     res = ch.readConfigSection("cassandra")
#     print(res)
#     print(type(res))