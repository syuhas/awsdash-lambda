#!/bin/bash

sudo dnf install zip -y    # install the zip utility
sudo dnf install python3-pip -y    # install the pip package manager

mkdir -p package    # create a directory to store the dependencies

pip install -r requirements.txt --target ./package  # install the dependencies in the package directory

cd package  # navigate to the package directory

zip -r9 ../lambda_function.zip .    # zip the contents of the package directory

cd ..   # navigate back to the original directory

zip -g lambda_function.zip lambda_function.py   # add the lambda function to the zip file