#!/bin/bash

./interactive_client -p 8888 < command_list_small1 > cl1&
./interactive_client -p 8888 < command_list_small2 > cl2&