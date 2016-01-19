#!/bin/bash
cd ext
ruby extconf.rb
make
cd ..
pwd
gem build cubrid.gemspec