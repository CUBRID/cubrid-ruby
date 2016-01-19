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
    
    def test_autocommit
        con = Cubrid.connect($database,$host,$port,$user,$password)
        if con
          #the default autocommit should be true
          assert_equal(true, con.auto_commit?)
        
          con.auto_commit = false
          assert_equal(false, con.auto_commit?)
          
          con.auto_commit = true
          assert_equal(true, con.auto_commit?)
          
          con.close      
        else
            puts "Connection could not be established"
        end
    end
end