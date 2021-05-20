#!/bin/bash

./interactive_client -p 8888 < command_list_small > cl1&
./interactive_client -p 8888 < command_list_small > cl2&
sudo lsof -i -P -n | grep LISTEN
