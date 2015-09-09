<?php
while(true){
$before = microtime(true);
$service = service_discovery_get("hello");
$after = microtime(true);
$timeCost = number_format($after - $before, 10);
echo "it takes " . $timeCost . " seconds\n";
echo "get service hello \n";
var_dump(service_discovery_get("hello"));
echo "get one service hello \n";
var_dump(service_discovery_get_one("hello"));
sleep(2);
}
