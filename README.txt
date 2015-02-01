/************************************ STEPS TO RUN 2D KMEANS ************************************/
mkdir WordCount_Classes
javac -classpath $HADOOP_HOME/hadoop-0.20.2-core.jar -d WordCount_Classes WordCount.java
jar -cvf WordCount.jar -C WordCount_Classes/ .
hadoop dfs -mkdir /user/hadoop/wordcount/input
hadoop dfs -copyFromLocal input.txt /user/hadoop/wordcount/input              NOTE THAT INPUT FILE MUST BE CALLED input.txt
hadoop dfs -rmr /user/hadoop/wordcount/output*
hadoop jar WordCount.jar WordCount /user/hadoop/dna/input /user/hadoop/dna/output <INSERT NUMBER OF CLUSTERS>


/************************************ STEPS TO RUN DNA KMEANS ************************************/
mkdir DNA_Classes
javac -classpath $HADOOP_HOME/hadoop-0.20.2-core.jar -d DNA_Classes WordCount.java
jar -cvf WordCount.jar -C DNA_Classes/ .
hadoop dfs -mkdir /user/hadoop/dna/input
hadoop dfs -copyFromLocal input.txt /user/hadoop/dna/input                    NOTE THAT INPUT FILE MUST BE CALLED input.txt
hadoop dfs -rmr /user/hadoop/dna/output*
hadoop jar WordCount.jar WordCount /user/hadoop/dna/input /user/hadoop/dna/output <INSERT NUMBER OF CLUSTERS>
