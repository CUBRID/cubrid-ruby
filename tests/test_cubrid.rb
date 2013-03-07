require 'test/unit'
require 'cubrid'

class CUBRID_Test < Test::Unit::TestCase
    def setup
        @con = Cubrid.connect('demodb')
    end

    def test_connect
        @con.auto_commit = true
        connection_info = @con.to_s
        puts connection_info
        assert( connection_info, "Connect failed.")
    end

    def test_query
        @con = Cubrid.connect('demodb')

        @con.query('drop table if exists test_cubrid')
        @con.query('create table test_cubrid (a int, b double, c string, d date)')
 
        @con.prepare('insert into test_cubrid values (?, ?, ?, ?)') { |stmt|
           stmt.bind(1, 10)
           stmt.bind(2, 3.141592)
           stmt.bind(3, 'hello')
           stmt.bind(4, Time.local(2007, 12, 25, 10, 10, 10), Cubrid::DATE)
           stmt.execute
        }

        stmt = @con.query('SELECT * FROM test_cubrid') 
        while row = stmt.fetch
            print row[0], " ", row[1],  " ", row[2], " ", row[3]
            puts 
        end
    end

    def test_column_info
        @con.prepare('SELECT * FROM db_user') { |stmt|
           stmt.execute
           stmt.column_info.each { |col|
             print col['name'], ' '
             print col['type_name'], ' '
             print col['precision'], ' '
             print col['scale'], ' '
             print col['nullable']
             puts
           }
        }
    end

    def test_each
        @con.prepare('SELECT * FROM db_user') { |stmt|
           stmt.execute
           stmt.each { |r|
             print r[0]
             puts
           }
        }
    end

    def test_each_hash
        @con.prepare('SELECT * FROM db_user') { |stmt|
           stmt.execute
           stmt.each_hash { |r|
             print r['name']
             puts
           }
        }
    end

    def teardown
        @con.close
    end
end

