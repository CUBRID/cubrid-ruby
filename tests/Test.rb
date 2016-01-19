print "string".object_id,"\n"#相同的字符串具有不同的id    
print "string".object_id,"\n"   
print "string".to_sym.object_id,"\n"  
print :string.object_id,"\n"#相同的符号具有相同的id    
print :string.object_id,"\n" 
print :string1.object_id,"\n" 
print :sss.to_sym,"\n" 


print :string.to_s,"\n"

a = :string
b = :string1

if a == b
  print "true","\n"
else
  print "false"
end

#print :65672.to_s