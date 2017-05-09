#!/bin/bash

# mvn package
MAIN_CLASS=$1
MAIN_CLASS=${MAIN_CLASS:-"SimpleJavaApp"}
spark-submit --class $MAIN_CLASS --master local[4] target/simple-java-project-1.0.jar
