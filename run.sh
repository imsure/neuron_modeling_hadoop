#!/bin/sh

hadoop fs -rm -r out-dir*
hadoop fs -rm -r firings
hadoop fs -rm -r in-dir
hadoop fs -mkdir in-dir
hadoop fs -put neurons.txt in-dir

hadoop jar neuron_modeling_hadoop-1.0.jar edu.stthomas.neuronhadoop.Model in-dir out-dir firings
