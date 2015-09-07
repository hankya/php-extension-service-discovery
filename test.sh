rm -f service-discovery.so main.o
make
make install
php test.php
