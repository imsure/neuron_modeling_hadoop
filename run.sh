#!/bin/sh

rm *.txt
hadoop fs -rm -r out-dir*
hadoop fs -rm -r firings
hadoop fs -rm -r potentials
hadoop fs -rm -r recoveries
hadoop fs -rm -r in-dir
hadoop fs -mkdir in-dir
hadoop fs -put input_by_py/neurons.txt in-dir

START=$(date +%s)
hadoop jar neuron_modeling_hadoop-1.0.jar edu.stthomas.neuronhadoop.Model -D mapred.reduce.tasks=10 in-dir out-dir firings potentials recoveries
END=$(date +%s)

TIME_DIFF=$(( $END - $START ))
echo "It took" $TIME_DIFF "seconds to run the Model." > running_time.txt

hadoop fs -get firings/part-r-00000 firings.txt
hadoop fs -get potentials/part-r-00000 potentials.txt
hadoop fs -get recoveries/part-r-00000 recoveries.txt

