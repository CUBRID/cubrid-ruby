require 'rubygems'
require 'cubrid'
db = 'demodb'
c = Cubrid.connect(db)
#con = Cubrid.connect('shard1','10.34.64.218',36069,'dba','')

c.auto_commit = false
#c.query('CREATE TABLE a (a integer)')
c.query('SELECT COUNT(*) FROM a').fetch
c.query('INSERT INTO a DEFAULT VALUES').fetch 
stmt = c.query('SELECT COUNT(*) FROM a') 
while row = stmt.fetch
    print row[0]
    puts 
end
c.rollback 

stmt = c.query('SELECT COUNT(*) FROM a') 
while row = stmt.fetch
    print row[0]
    puts 
end