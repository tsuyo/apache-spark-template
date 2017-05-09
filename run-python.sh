#!/bin/bash

MAIN_CLASS=$1
MAIN_CLASS=${MAIN_CLASS:-"SimplePythonApp.py"}
spark-submit --master local[4] src/main/python/$MAIN_CLASS
