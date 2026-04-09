#!/bin/bash

docker build --push -t 884080474326.dkr.ecr.eu-west-3.amazonaws.com/opac-devops:step-anonymisation -f Dockerfile-anonymisation .
docker build --push -t 884080474326.dkr.ecr.eu-west-3.amazonaws.com/opac-devops:step-drifting -f Dockerfile-drifting .
docker build --push -t 884080474326.dkr.ecr.eu-west-3.amazonaws.com/opac-devops:step-rename-dance -f Dockerfile-rename-dance .