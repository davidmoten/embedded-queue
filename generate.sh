#!/bin/bash
cd embedded-queue-definition/ && mvn clean install  && cd ../embedded-queue-generated/ &&  mvn clean install  && cd .. && yed embedded-queue-generated/target/state-machine-docs/org.davidmoten.eq.model.Reader.graphml
