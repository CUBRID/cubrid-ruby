require 'rubygems'
require 'test/unit'
require 'cubrid'
require 'yaml'
        
class CUBRID_Test_Connection < Test::Unit::TestCase
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

    def test_connect
        puts "test_connect:"
        assert(@con, "Connect failed.")
    end
    
    def test_close
        puts "test_close:"
        @con2 = Cubrid.connect($database,$host,$port,$user,$password)
        assert(@con2, "Connect failed.")
        
        # close the connection
        @con2.close
        exception = assert_raise(Cubrid::Error) {@con2.query('create table testtbl (a int)')}
        #assert_match("ERROR: CAS, -2, Invalid connection handle", exception.message) 
    end
    
    def test_to_s
        puts "test_to_s:"
        connection_info = @con.to_s
        assert_equal("host: #{$host}, port: #{$port}, db: #{$database}, user: #{$user}", connection_info)
    end
    
    def test_server_version
        puts "test_server_version:"
        s_version = @con.server_version
        assert_match("10.0.0.1285", s_version)
    end
    
    def test_autocommit
        puts "test_autocommit:"
        if @con
          #the default autocommit should be true
          assert_equal(true, @con.auto_commit?)
        
          @con.auto_commit = false
          assert_equal(false, @con.auto_commit?)
          
          @con.auto_commit = true
          assert_equal(true, @con.auto_commit?)       
        end
    end

    def test_query
        puts "test_query:"
        # create table test_cubrid
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query("CREATE TABLE test_cubrid (a INT, b DOUBLE, c STRING, d DATE)")
        
        # insert one row data
        @con.query("INSERT INTO test_cubrid values (10, 3.14, 'HELLO', '2013-10-09')")
        
        # select query
        stmt = @con.query('SELECT * FROM test_cubrid') 
        while row = stmt.fetch
            assert_equal(10, row[0]);
            assert_equal(3.14, row[1]);
            assert_equal('HELLO', row[2]);
            assert_equal('2013-10-09', row[3].strftime("%Y-%m-%d"));
        end    
    end
    
    def test_commit
        puts "test_commit:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query("CREATE TABLE test_cubrid (a INT, b DOUBLE, c STRING, d DATE)")
        
        # insert data without commit
        @con1 = Cubrid.connect($database,$host,$port,$user,$password)
        @con1.auto_commit = false
        @con1.query("INSERT INTO test_cubrid values (11, 3.14, 'HELLO', '2013-10-09')")
        @con1.close
        
        @con2 = Cubrid.connect($database,$host,$port,$user,$password)
        stmt = @con2.query('SELECT * FROM test_cubrid')
        assert_equal(0, stmt.affected_rows); 
        
        # insert data with commit
        @con1 = Cubrid.connect($database,$host,$port,$user,$password)
        @con1.auto_commit = false
        @con1.query("INSERT INTO test_cubrid values (11, 3.14, 'HELLO', '2013-10-09')")
        @con1.commit
        @con1.close
        
        @con2 = Cubrid.connect($database,$host,$port,$user,$password)
        stmt = @con2.query('SELECT * FROM test_cubrid')
        assert_equal(1, stmt.affected_rows); 
    end

    def test_rollback
        puts "test_rollback:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query("CREATE TABLE test_cubrid (a INT, b DOUBLE, c STRING, d DATE)")
        
        @con.auto_commit = false
        @con.query("INSERT INTO test_cubrid values (11, 3.14, 'HELLO', '2013-10-09')")
        stmt = @con.query('SELECT * FROM test_cubrid')
        assert_equal(1, stmt.affected_rows); 
        
        # after rollback
        @con.rollback
        stmt = @con.query('SELECT * FROM test_cubrid')
        assert_equal(0, stmt.affected_rows); 
    end

    def test_prepare
        puts "test_prepare:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query("CREATE TABLE test_cubrid (a INT, b DOUBLE, c STRING, d DATE,e bigint,f bit)")
        
        @con.prepare('insert into test_cubrid values (?, ?, ?, ?,?,?)') { |stmt|
            stmt.execute(10, 3.14, 'hello', Time.local(2013, 10, 9),1234567890,'1')
        }
        
        stmt = @con.query('SELECT * FROM test_cubrid') 
        while row = stmt.fetch
            assert_equal(10, row[0]);
            assert_equal(3.14, row[1]);
            assert_equal('hello', row[2]);
            assert_equal('2013-10-09', row[3].strftime("%Y-%m-%d"));
        end      
        
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query("CREATE TABLE test_cubrid (d DATE,f bit,t time, dt datetime,ts timestamp,s string)") 
        stmt =@con.prepare('insert into test_cubrid values (?, ?, ?, ?,?,?)')
        
        stmt.bind(1,'2012-10-01')
        stmt.bind(2,'1',5)
        stmt.bind(3,'01:01:01',14)
        stmt.bind(4,'2010-01-01 00:00:00.001',22)
        stmt.bind(5,'2010-01-01 00:00:01',15)
        stmt.bind(6,0)
        stmt.execute    
        stmt = @con.query('SELECT * FROM test_cubrid')         
    end
    
    def test_set
      puts "test_set:"
      @con.query("DROP TABLE IF EXISTS test_cubrid")
      @con.query("CREATE TABLE test_cubrid(a SET(INT),
        b SET(DOUBLE),c SET(BIGINT),s SET(string),d SET(DATE),dt SET(datetime),e SET(bit))")
      
      sData=["abc","def","ghj","klm"]     
      stmt = @con.prepare('insert into test_cubrid values (?,?,?,?,?,?,?)')    
      stmt.bind(1,[1,2,3])
      stmt.bind(2,[1.1,2.2])
      stmt.bind(3,[1234567789,0])
      stmt.bind(4,sData)
      stmt.bind(5,[Time.local(2013, 10, 9)])
      stmt.bind(6,['2010-01-01 00:00:00'])
      stmt.bind(7,['1'],16,5) 
      stmt.execute
      
      stmt = @con.query('SELECT * FROM test_cubrid') 
      while row = stmt.fetch
        puts row[0];
      end  
    end
    
    def teardown
        @con.close
    end
end

