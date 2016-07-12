<?php

include('action.php');

$in_json = '{ "name": "Steve" }';

$in_obj = json_decode($in_json, true);

$out_obj = main($in_obj);

$out_json = json_encode($out_obj);

echo $out_json;

?>
