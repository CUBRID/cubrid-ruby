require 'rubygems'
require 'cubrid'
require 'test/unit'
require 'yaml'

class CUBRID_APIS_291 < Test::Unit::TestCase
    file = open('config.cfg')
    cfg = YAML.load(file)
    
    $database = cfg["database"]
    $host = cfg["host"]
    $port = cfg["port"]
    $user = cfg["user"]
    $password = cfg["password"]
    
    def test_last_insert_id
        con = Cubrid.connect($database,$host,$port,$user,$password)
        if con
            con.query("drop table if exists test_cubrid")
            con.query('create table test_cubrid(id NUMERIC AUTO_INCREMENT(10300, 1), name VARCHAR(50))')
            con.query('insert into test_cubrid(name) values (\'Lily\')')
            assert_equal(10300, con.last_insert_id())
            con.close()
        else
            puts "Connection could not be established"
        end
    end
end