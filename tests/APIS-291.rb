require 'rubygems'
require 'cubrid'
db = 'demodb'
c = Cubrid.connect(db)
#con = Cubrid.connect('shard1','10.34.64.218',36069,'dba','')

#c.auto_commit = false
c.query('create table test_cubrid(id NUMERIC AUTO_INCREMENT(10300, 1), name VARCHAR(50))')
#c.query('SELECT COUNT(*) FROM a').fetch
c.query('insert into test_cubrid(name) values (\'Lily\')')
puts c.last_insert_id()
