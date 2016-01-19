#!/bin/bash

result="tests.result"
touch $result

echo -e "**********************************" > $result
echo -e "******Ruby Driver Unit Test*******" >> $result
echo -e "**********************************" >> $result
echo -e "\n" >> $result

i=1
for test_file in $(ls *.rb); do
	echo -e "********$i. Test for $test_file ********" >> $result
	#run ruby gems to test all the unittests in the folder
	ruby -rubygems $test_file >> $result
	sleep 1
	echo -e "\n" >> $result
	i=`expr $i + 1`
done
