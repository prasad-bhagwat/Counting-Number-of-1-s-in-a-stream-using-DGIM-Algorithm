Estimating number of 1's in a 0/1 data stream using DGIM Algorithm
=====================================================


### Enviroment versions required:

Spark: 2.2.1  
Python: 2.7  
Scala: 2.11


### Algorithm implementation approach:

The code connects on _port 9999_ of _localhost_ listening for 0/1 data stream which is used by DGIM algorithm to estimate number of 1's. Every 10 seconds, while you get batch data in spark streaming we print out the number of the estimated and actual count of 1's. The percentage error of the estimated result is less than 50%.  
_NOTE_: First start generating the data stream of 0/1 on _port 9999_ of _localhost_ then run the program as follows:

### Python command for estimating number of 1's using DGIM Algorithm

* * *

Estimating number of 1's using _“Prasad\_Bhagwat\_DGIMAlgorithm.py”_ file

    spark-submit Prasad_Bhagwat_DGIMAlgorithm.py
    

Example usage of the above command is as follows:  

     ~/Desktop/spark-2.2.1/bin/spark-submit Prasad_Bhagwat_DGIMAlgorithm.py


### Scala command for estimating number of 1's using DGIM Algorithm

* * *

Estimating number of 1's using _“Prasad\_Bhagwat\_DGIMAlgorithm.jar”_ file

    spark-submit --class DGIMAlgorithm Prasad_Bhagwat_DGIMAlgorithm.jar
    

Example usage of the above command is as follows:

     ~/Desktop/spark-2.2.1/bin/spark-submit --class DGIMAlgorithm Prasad_Bhagwat_DGIMAlgorithm.jar


### Sample Output:

Estimated number of ones in the last 1000 bits: 475  
Actual number of ones in the last 1000 bits: 513  
