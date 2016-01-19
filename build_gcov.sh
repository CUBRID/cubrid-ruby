#!/bin/bash
cd ext
rm -r -f *.gcno
rm -r -f *.gcda
ruby extconf.rb daily
make clean
make install
cd  ../unittest
chmod +x ./run_test.sh
./run_test.sh
cd ../ext
lcov --capture --directory . --output-file test.info --test-name test
genhtml test.info --output-directory output --title "cubrid unittest(ruby driver)" --show-details --legend
pwd