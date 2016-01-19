require 'rubygems'
require 'test/unit'
require 'cubrid'
require 'yaml'
        
class CUBRID_Test_Exception < Test::Unit::TestCase
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

    def test_connection_exception
        puts "test_connection_exception:"
        exception = assert_raise(Cubrid::Error) {con = Cubrid.connect('invalid_db',$host,$port,$user,$password)}
        assert_match("ERROR: DBMS, -677, Failed to connect to database server, 'invalid_db'", exception.message)
        
        exception = assert_raise(Cubrid::Error) {con = Cubrid.connect($database,'invalid_host',$port,$user,$password)}
        #assert_match("ERROR: CAS, -2, Invalid connection handle", exception.message)
        
        exception = assert_raise(Cubrid::Error) {con = Cubrid.connect($database,$host,11111,$user,$password)}
        assert_match("ERROR: CAS, -16, Connection error", exception.message)
        
        exception = assert_raise(Cubrid::Error) {con = Cubrid.connect($database,$host,$port,'invalid_user',$password)}
        assert_match("ERROR: DBMS, -165, User \"invalid_user\" is invalid.", exception.message)
        
        exception = assert_raise(Cubrid::Error) {con = Cubrid.connect($database,$host,$port,$user,'invalid_password')}
        assert_match("ERROR: DBMS, -171, Incorrect or missing password.", exception.message)
        
        exception = assert_raise(Cubrid::Error) {@con.rollback}
    end
    
    def test_connection_close
        puts "test_connection_close:"
        @con2 = Cubrid.connect($database,$host,$port,$user,$password)
        assert(@con2, "Connect failed.")
        
        # close the connection
        @con2.close
        exception = assert_raise(Cubrid::Error) {@con2.query('create table testtbl (a int)')}
        #assert_match("ERROR: CAS, -2, Invalid connection handle", exception.message) 
    end
    
    def test_invalid_query
        puts "test_invalid_query:"
        exception = assert_raise(Cubrid::Error) {@con.query('invalid sql query statament')}
        assert_match("ERROR: DBMS, -493, Syntax: syntax error, unexpected IdName", exception.message)    
    end
    
    def test_invalid_prepare
        puts "test_invalid_prepare:"
        exception = assert_raise(Cubrid::Error) {@con.prepare('invalid sql prepare statament')}
        assert_match("ERROR: DBMS, -493, Syntax: syntax error, unexpected IdName", exception.message) 
        exception = assert_raise(Cubrid::Error) {@con.prepare()}
    end 

    def test_invalid_prepare_semantic
      puts "test_invalid_prepare_semantic:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (a int, b double, c date)')
        
        stmt = @con.prepare('INSERT INTO test_cubrid VALUES (?,?)') 
        stmt.bind(1, 100)
        exception = assert_raise(Cubrid::Error) {stmt.execute}
        assert_match("ERROR: DBMS, -494, Semantic: The number of attributes(3) and values(2) are not equal.", exception.message)  
    end
    
    def test_bind_invalid_datatype
        puts "test_bind_invalid_datatype:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (a int, b double, c date)')
        
        stmt = @con.prepare('INSERT INTO test_cubrid VALUES (?,?,?)') 
        stmt.bind(1, 'invalid_int_value')
        exception = assert_raise(Cubrid::Error) {stmt.execute}
        assert_match("ERROR: DBMS, -494, Semantic: Cannot coerce host var to type integer.", exception.message) 
        
        stmt.bind(1, 100)  
        stmt.bind(2, 'invalid_double_value')
        exception = assert_raise(Cubrid::Error) {stmt.execute} 
        assert_match("ERROR: DBMS, -494, Semantic: Cannot coerce host var to type double.", exception.message)
        
        stmt.bind(1, 100)  
        stmt.bind(2, 3.14) 
        stmt.bind(3, 'invalid_date_value')   
        exception = assert_raise(Cubrid::Error) {stmt.execute} 
        assert_match("ERROR: DBMS, -494, Semantic: Cannot coerce host var to type date.", exception.message)
        
        exception = assert_raise(Cubrid::Error) {@con.prepare('INSERT INTO test_cubrid VALUE (?,?,?)')}
    end
    
    def test_execute_invalid_argument
        puts "test_execute_invalid_argument:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (a int, b double, c date)')
        
        stmt = @con.prepare('INSERT INTO test_cubrid VALUES (?,?,?)') 
        exception = assert_raise(StandardError) {stmt.execute(100)}
        assert_equal("execute: param_count(3) != number of argument(1)", exception.message)
    end
    
    def test_execute_invalid_datatype
        puts "test_execute_invalid_argument:"
        @con.query("DROP TABLE IF EXISTS test_cubrid")
        @con.query('CREATE TABLE test_cubrid (a int, b double, c date)')
        stmt = @con.prepare('INSERT INTO test_cubrid VALUES (?,?,?)') 
        
        exception = assert_raise(Cubrid::Error) {stmt.execute('invalid_int_value', 3.14, Time.local(2013, 10, 11, 10, 10, 10))}
        assert_match("ERROR: DBMS, -494, Semantic: Cannot coerce host var to type integer.", exception.message)

        exception = assert_raise(Cubrid::Error) {stmt.execute(100, 'invalid_double_value', Time.local(2013, 10, 11, 10, 10, 10))}
        assert_match("ERROR: DBMS, -494, Semantic: Cannot coerce host var to type double.", exception.message)
        
        exception = assert_raise(Cubrid::Error) {stmt.execute(100, 3.14, 'invalid_date_value')}
        assert_match("ERROR: DBMS, -494, Semantic: Cannot coerce host var to type date.", exception.message)
    end
    
    def teardown
        @con.close
    end
end