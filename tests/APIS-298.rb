require 'rubygems'
require 'cubrid'
db = 'demodb'
#c = Cubrid.connect(db)
c = Cubrid.connect(db,'127.0.0.1',30000,'dba','')

#c.query('CREATE TABLE a (a integer)')
#c.query('SELECT COUNT(*) FROM a').fetch
#c.query('INSERT INTO a DEFAULT VALUES').fetch 
stmt = c.query('SELECT * FROM t_type_int32_overbound') 
while row = stmt.fetch
    print row[0]
    puts 
    print row[1]
    puts
end

puts stmt.affected_rows