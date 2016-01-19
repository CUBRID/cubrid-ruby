require 'rubygems'
require 'cubrid'
require 'test/unit'
require 'yaml'

class CUBRID_APIS_678 < Test::Unit::TestCase
    file = open('config.cfg')
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
          con.query("CREATE TABLE tbl (a INT, b VARCHAR)")
         
          stmt = con.prepare('insert into tbl values (?, ?)') 
          stmt.execute(1,"aa")
      
          stmt = con.prepare('insert into tbl values (?, ?)') 
          stmt.bind(1,2)
          stmt.bind(2,'bb')
          stmt.execute
          
          answer = [1, 'aa', 2, 'bb']
          result = Array.new
          
          stmt = con.query('SELECT * FROM tbl') 
          while row = stmt.fetch
              result.push(row[0])
              result.push(row[1])
          end    
          
          assert_equal(answer, result)
                    
        else
            puts "Connection could not be established"
        end
    end
end