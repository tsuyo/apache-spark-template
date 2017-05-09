#!/bin/bash

# sbt package
MAIN_CLASS=$1
MAIN_CLASS=${MAIN_CLASS:-"SimpleScalaApp"}
spark-submit --class $MAIN_CLASS --master local[4] target/scala-2.11/simple-scala-project_2.11-1.0.jar
