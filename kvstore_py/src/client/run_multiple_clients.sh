#!/bin/bash

./interactive_client -p 8888 < command_list_put &
rm results
# ./interactive_client -p 8888 < command_list &

./interactive_client -p 8888 < command_list &
./interactive_client -p 8888 < command_list &
# ./interactive_client -p 8888 < command_list &
# ./interactive_client -p 8888 < command_list &