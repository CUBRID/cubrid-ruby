require 'rubygems'
require 'test/unit'
require 'cubrid'
require 'yaml'
        
class CUBRID_Test_Statement < Test::Unit::TestCase
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

    def test_last_insert_id
        puts "test_last_insert_id"
        puts "connect",@con.last_insert_id
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('create table test_cubrid(id NUMERIC AUTO_INCREMENT(10300, 1), name VARCHAR(50))')
        
        @con.query('insert into test_cubrid(name) values (\'Lily\')')
        assert_not_equal(@con.last_insert_id,0);
    end

    def test_affected_rows
        puts "test_affected_rows:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query("CREATE TABLE test_cubrid (id INT, name STRING)")
        
        stmt = @con.query("INSERT INTO test_cubrid VALUES (1, 'Lily')")
        assert_equal(1, stmt.affected_rows)
        stmt = @con.query("INSERT INTO test_cubrid VALUES (2, 'Lucy'),(3, 'Sue'),(4, 'Paul')")
        assert_equal(3, stmt.affected_rows)
        
        stmt = @con.query("UPDATE test_cubrid SET name = 'May' WHERE id = 3 or name = 'Lily'")
        assert_equal(2, stmt.affected_rows)
        
        stmt = @con.query("SELECT * FROM test_cubrid")
        assert_equal(4, stmt.affected_rows)
        stmt = @con.query("SELECT * FROM test_cubrid where name = 'May'")
        assert_equal(2, stmt.affected_rows)
        
        stmt = @con.query("DELETE FROM test_cubrid where id <= 3")
        assert_equal(3, stmt.affected_rows)
    end
    
    def test_bind
        puts "test_bind:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (a int, b double, c string, d date)')
        
        @con.prepare('INSERT INTO test_cubrid VALUES (?, ?, ?, ?)') { |stmt|
            stmt.bind(1, 10)
            stmt.bind(2, 3.14)
            stmt.bind(3, 'hello')
            stmt.bind(4, Time.local(2013, 10, 11, 10, 10, 10), Cubrid::DATE)
            stmt.execute
        }
        
        @con.prepare('SELECT * FROM test_cubrid where a = ?') { |stmt|
            stmt.bind(1, 10)
            stmt.execute          
            while row = stmt.fetch
              assert_equal(10, row[0]);
              assert_equal(3.14, row[1]);
              assert_equal('hello', row[2]);
              assert_equal('2013-10-11', row[3].strftime("%Y-%m-%d"));
            end  
        }  
    end
 
    def test_close
        puts "test_close:"
        stmt = @con.query("SELECT * FROM db_class")
        assert_not_equal(0, stmt.affected_rows)
        
        stmt.close
        exception = assert_raise(Cubrid::Error) {stmt.affected_rows}
        #assert_equal("ERROR: CAS, -18, Invalid request handle", exception.message)
    end

    def test_column_info
        puts "test_column_info:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (col_1 INT NOT NULL, col_2 VARCHAR(256), col_3 DATE, col_4 SEQUENCE)')
        stmt = @con.query("SELECT * FROM test_cubrid")
        stmt.execute
        
        assert(stmt.column_info, "Get statement column info failed.")
        
        col_name_answer = ['col_1','col_2','col_3','col_4']
        col_name_result = Array.new;
        
        col_precision_answer = [0,256,0,0]
        col_precision_result = Array.new;
        
        col_null_answer = [1,0,0,0]
        col_null_result = Array.new;
        
        stmt.column_info.each { |col|
          col_name_result.push(col['name'])
          col_precision_result.push(col['precision'])
          col_null_result.push(col['nullable'])
        }
        
        assert_equal(col_name_answer, col_name_result)
        assert_equal(col_precision_answer, col_precision_result)
        assert_equal(col_null_answer, col_null_result)
    end
    
    def test_each
        puts "test_each:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (id INT, name STRING)')
        @con.query("INSERT INTO test_cubrid VALUES (1, 'Lucy'),(2, 'Sue')")
        
        stmt = @con.query("SELECT * FROM test_cubrid")
        i = 1;
        stmt.each { |r|
           if i == 1
              assert_equal(1, r[0])
              assert_equal('Lucy', r[1])
           else
              assert_equal(2, r[0])
              assert_equal('Sue', r[1])
           end
           i = i + 1
        }
    end
    
    def test_each_hash
        puts "test_each_hash:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (id INT, name STRING)')
        @con.query("INSERT INTO test_cubrid VALUES (1, 'Lucy'),(2, 'Sue')")
        
        stmt = @con.query("SELECT * FROM test_cubrid")
        i = 1;
        stmt.each_hash { |r|
           if i == 1
              assert_equal(1, r['id'])
              assert_equal('Lucy', r['name'])
           else
              assert_equal(2, r['id'])
              assert_equal('Sue', r['name'])
           end
           i = i + 1
        }
    end

    def test_execute
        puts "test_execute:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (col_1 INT, col_2 INT)')
        stmt = @con.prepare("INSERT INTO test_cubrid VALUES (?, ?)")
        
        result = stmt.execute(1, 100)
        assert_equal(1, result)
        
        stmt = @con.query("SELECT * FROM test_cubrid")
        assert_equal(1, stmt.affected_rows)
        stmt.each_hash { |r|
              assert_equal(1, r['col_1'])
              assert_equal(100, r['col_2'])
        }
    end
    
    def test_execute_invalid_type
        puts "test_execute_invalid_type:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (col_1 INT, col_2 INT)')
        stmt = @con.prepare("INSERT INTO test_cubrid VALUES (?, ?)")
        
        exception = assert_raise(Cubrid::Error) {stmt.execute(1, 'text')}
        assert_match("ERROR: DBMS, -494, Semantic: Cannot coerce host var to type integer.", exception.message)
    end
    
    def test_execute_bind
        puts "test_execute_bind:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (id INT, name STRING)')
        stmt = @con.prepare("INSERT INTO test_cubrid VALUES (?, ?)")
        stmt.bind(1, 1)
        stmt.bind(2, 'Lily')
        
        stmt1 = @con.query("SELECT * FROM test_cubrid")
        assert_equal(0, stmt1.affected_rows)
        
        result = stmt.execute
        stmt1 = @con.query("SELECT * FROM test_cubrid")
        assert_equal(1, stmt1.affected_rows)
    end
    
    def test_fetch
        puts "test_fetch:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (id INT, name STRING)')
        @con.query("INSERT INTO test_cubrid VALUES (1, 'Lily'),(2, 'Sue'),(3, 'Lucy')")
        
        answer = [1, 'Lily', 2, 'Sue', 3, 'Lucy']
        result = Array.new
        
        stmt = @con.query("SELECT * FROM test_cubrid")
        while rows = stmt.fetch
          result.push(rows[0])
          result.push(rows[1])
        end
        
        assert_equal(answer, result)
    end

    def test_fetch_hash
        puts "test_fetch_hash:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (id INT, name STRING)')
        @con.query("INSERT INTO test_cubrid VALUES (1, 'Lily'),(2, 'Sue'),(3, 'Lucy')")
        
        answer = [1, 'Lily', 2, 'Sue', 3, 'Lucy']
        result = Array.new
        
        stmt = @con.query("SELECT * FROM test_cubrid")
        while rows = stmt.fetch_hash
          result.push(rows['id'])
          result.push(rows['name'])
        end
        
        assert_equal(answer, result)
    end
    
    def teardown
        @con.close
    end
end