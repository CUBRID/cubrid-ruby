require 'rubygems'
require 'cubrid'
require 'test/unit'
require 'yaml'

class CUBRID_APIS_708 < Test::Unit::TestCase
    file = open('config.cfg')
    cfg = YAML.load(file)
    
    $database = cfg["database"]
    $host = cfg["host"]
    $port = cfg["port"]
    $user = cfg["user"]
    $password = cfg["password"]
    
    def test_lastInsertId_int
        con = Cubrid.connect($database,$host,$port,$user,$password)
        if con
          con.query("DROP TABLE IF EXISTS test_cubrid")
          con.query("CREATE TABLE test_cubrid (id INT AUTO_INCREMENT(1,1), name STRING)")
          
          con.query("INSERT INTO test_cubrid (name) values ('jerry')")
          last_id = con.last_insert_id()
          puts last_id.class
        
          assert_equal(1,last_id);
          
          con.query("INSERT INTO test_cubrid (name) values ('paul')")
          last_id = con.last_insert_id()
          puts last_id.class
        
          assert_equal(2,last_id);
          
          con.query("INSERT INTO test_cubrid (name) values ('apple'),('pear')")
          last_id = con.last_insert_id()
          puts last_id.class
        
          assert_equal(3,last_id);
          
          con.close          
        else
            puts "Connection could not be established"
        end
    end
    
    def test_lastInsertId_smallInt
        con = Cubrid.connect($database,$host,$port,$user,$password)
        if con
          con.query("DROP TABLE IF EXISTS test_cubrid")
          con.query("CREATE TABLE test_cubrid (id SMALLINT AUTO_INCREMENT(1,1), name STRING)")
          
          con.query("INSERT INTO test_cubrid (name) values ('jerry')")
          last_id = con.last_insert_id()
          puts last_id.class
        
          assert_equal(1,last_id);
          
          con.query("INSERT INTO test_cubrid (name) values ('apple'),('pear')")
          last_id = con.last_insert_id()
          puts last_id.class
        
          assert_equal(2,last_id);
          
          con.close          
        else
            puts "Connection could not be established"
        end
    end
    
    def test_lastInsertId_bigInt
        con = Cubrid.connect($database,$host,$port,$user,$password)
        if con
          con.query("DROP TABLE IF EXISTS test_cubrid")
          con.query("CREATE TABLE test_cubrid (id BIGINT AUTO_INCREMENT(1000000,1), name STRING)")
          
          con.query("INSERT INTO test_cubrid (name) values ('jerry')")
          last_id = con.last_insert_id()
          puts last_id.class
        
          assert_equal(1000000,last_id);
          
          con.query("INSERT INTO test_cubrid (name) values ('apple'),('pear')")
          last_id = con.last_insert_id()
          puts last_id.class
        
          assert_equal(1000001,last_id);
          
          con.close          
        else
            puts "Connection could not be established"
        end
    end
    
    def test_lastInsertId_numeric
        con = Cubrid.connect($database,$host,$port,$user,$password)
        if con
          con.query("DROP TABLE IF EXISTS test_cubrid")
          con.query("CREATE TABLE test_cubrid (id NUMERIC(4,0) AUTO_INCREMENT(1,1), name STRING)")
          
          con.query("INSERT INTO test_cubrid (name) values ('jerry')")
          last_id = con.last_insert_id()
          puts last_id.class
        
          assert_equal(1,last_id);
          
          con.query("INSERT INTO test_cubrid (name) values ('apple'),('pear')")
          last_id = con.last_insert_id()
          puts last_id.class
        
          assert_equal(2,last_id);
          
          con.close          
        else
            puts "Connection could not be established"
        end
    end
end