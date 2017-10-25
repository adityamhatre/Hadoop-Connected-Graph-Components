@echo off
hadoop fs -rm -r -skipTrash /graph/out /graph/int
hadoop jar "Graph Analysis.jar" edu.uta.cse6331.Graph /graph/input/file01 /graph/int /graph/out