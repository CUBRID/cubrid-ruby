require 'rubygems'
require 'cubrid'
require 'test/unit'
require 'yaml'

class CUBRID_APIS_683 < Test::Unit::TestCase
    file = open('config.cfg')
    cfg = YAML.load(file)
    
    $database = cfg["database"]
    $host = cfg["host"]
    $port = cfg["port"]
    $user = cfg["user"]
    $password = cfg["password"]
    
    def test_affectedrows
        con = Cubrid.connect($database,$host,$port,$user,$password)
        if con
          con.query("DROP TABLE IF EXISTS tbl")
          con.query("CREATE TABLE tbl (id INT, name STRING)")
         
          stmt = con.query("INSERT INTO tbl VALUES (1, 'Lily'),(2, 'Lucy')")
          assert_equal(2, stmt.affected_rows)
          stmt = con.query("UPDATE tbl set name = 'Mario' WHERE name = 'Lucy'")
          assert_equal(1, stmt.affected_rows)
          stmt = con.query("DELETE FROM tbl")
          assert_equal(2, stmt.affected_rows)
          
          con.close
                    
        else
            puts "Connection could not be established"
        end
    end
end