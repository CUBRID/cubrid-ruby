require 'rubygems'
require 'test/unit'
require 'cubrid'
require 'yaml'
        
class CUBRID_Test_Transaction < Test::Unit::TestCase
    file = open('config.cfg')
    cfg = YAML.load(file)
    
    $database = cfg["database"]
    $host = cfg["host"]
    $port = cfg["port"]
    $user = cfg["user"]
    $password = cfg["password"]
        
    def setup
        @con = Cubrid.connect($database,$host,$port,$user,$password)
    end

    def test_transaction_basic
        puts "test_transaction_basic:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (id int, name string)')
        
        @con.auto_commit = false
        @con.query("INSERT INTO test_cubrid values (1, 'Lily'),(2,'Lucy')")
        @con.commit
        
        stmt = @con.query('SELECT * FROM test_cubrid')
        assert_equal(2, stmt.affected_rows);
        
        answer = [1, 'Lily', 2, 'Lucy']
        result = Array.new
        
        while rows = stmt.fetch_hash
          result.push(rows['id'])
          result.push(rows['name'])
        end
        
        assert_equal(answer, result)
    end
    
    def test_transaction_rollback
        puts "test_transaction_rollback:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (id int, name string)')
        
        @con.auto_commit = false
        @con.query("INSERT INTO test_cubrid values (1, 'Lily'),(2,'Lucy')")
        @con.rollback
        
        stmt = @con.query('SELECT * FROM test_cubrid')
        assert_equal(0, stmt.affected_rows);
        
        result = Array.new
        while rows = stmt.fetch_hash
          result.push(rows['id'])
          result.push(rows['name'])
        end
        
        assert_equal(0, result.count)
    end
    
    def test_transaction_multiple_process
        puts "test_transaction_multiple_process:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (id int, name string)')
        
        @con.auto_commit = false
        @con.query("INSERT INTO test_cubrid values (1, 'Lily'),(2,'Lucy')")
        
        @con2 = Cubrid.connect($database,$host,$port,$user,$password)
        stmt = @con2.query('SELECT * FROM test_cubrid')
        assert_equal(0, stmt.affected_rows);
  
        @con.commit
        stmt = @con2.query('SELECT * FROM test_cubrid')
        assert_equal(2, stmt.affected_rows);
        
        @con2.close     
    end
    
    def test_transaction_rollback_multiple
        puts "test_transaction_rollback_multiple:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (id int, name string)')
        
        @con.auto_commit = false
        @con.query("INSERT INTO test_cubrid values (1, 'Lily'),(2,'Lucy')")
        stmt = @con.query('SELECT * FROM test_cubrid')
        assert_equal(2, stmt.affected_rows);
        
        @con2 = Cubrid.connect($database,$host,$port,$user,$password)
        stmt = @con2.query('SELECT * FROM test_cubrid')
        assert_equal(2, stmt.affected_rows);
        
        @con.rollback
        
        stmt = @con2.query('SELECT * FROM test_cubrid')
        assert_equal(0, stmt.affected_rows);
        
        @con2.close     
    end
    
    
    def teardown
        @con.close
    end
end