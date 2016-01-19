require 'rubygems'
require 'cubrid'
db = 'demodb'
#con = Cubrid.connect(db)
con = Cubrid.connect('shard1','10.34.64.218',36069,'dba','')
if con
    puts "connection established"
    puts "CUBRID Database version is: #{con.server_version}"
    con.close()
else
    puts "Connection could not be established"
end