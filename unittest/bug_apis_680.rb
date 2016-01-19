require 'rubygems'
require 'cubrid'
require 'test/unit'
require 'yaml'

class CUBRID_APIS_680 < Test::Unit::TestCase
    file = open('../../../config.cfg')
    cfg = YAML.load(file)
    
    $database = cfg["database"]
    $host = cfg["host"]
    $port = cfg["port"]
    $user = cfg["user"]
    $password = cfg["password"]
    
    def test_prepare_string
        con = Cubrid.connect($database,$host,$port,$user,$password)
        if con
          con.query("DROP TABLE IF EXISTS tbl")
          con.query("CREATE TABLE tbl (col_1 INT, col_2 STRING, col_3 VARCHAR, col_4 DATE)")
          con.query("INSERT INTO tbl VALUES(1, 'aa', 'bb', '2013-10-18')")
             
          stmt = con.query("SELECT * FROM tbl")
          
          answer = ['INT', 'STRING', 'STRING', 'DATE']
          result = Array.new
          
          stmt.column_info.each { |col|
            result.push(col['type_name'])
          }
          
          assert(answer, result)
                    
        else
            puts "Connection could not be established"
        end
    end
end