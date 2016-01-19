require 'rubygems'
require 'cubrid'
require 'test/unit'
require 'yaml'

class CUBRID_APIS_684 < Test::Unit::TestCase
    file = open('config.cfg')
    cfg = YAML.load(file)
    
    $database = cfg["database"]
    $host = cfg["host"]
    $port = cfg["port"]
    $user = cfg["user"]
    $password = cfg["password"]
    
    def test_execute_multiple_times
        con = Cubrid.connect($database,$host,$port,$user,$password)
        if con
          con.query("DROP TABLE IF EXISTS tbl")
          con.query("CREATE TABLE tbl (id INT, name STRING)")
          
          stmt = con.prepare('insert into tbl values (?, ?)')
          stmt.execute(100, 'Lily')
          stmt.execute(101, 'Lucy')
          
          stmt = con.query('select * from tbl')
          assert_equal(2, stmt.affected_rows)
          
          con.close          
        else
            puts "Connection could not be established"
        end
    end
end