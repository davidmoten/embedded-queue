#!/bin/bash
mvn clean install -pl embedded-queue-generated -am  &&  yed embedded-queue-generated/target/state-machine-docs/org.davidmoten.eq.model.Reader.graphml
