# Imports required for the program
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import math
import random
from collections import deque


# Global variable declarations
current_timestamp       = 0
reservoir_size		= 1000
buckets_queue           = [deque() for i in range(int(math.ceil(math.log(reservoir_size) / math.log(2))) - 1)]
oldest_bucket_timestamp = -1
input_data_window       = deque()


# Checking whether to discard the oldest bucket or not
def discard_oldest_bucket_check(last_bucket_timestamp):
    return True if (current_timestamp - last_bucket_timestamp) % (reservoir_size * 2) >= reservoir_size else False


# Discarding the oldest bucket from bucket queue
def discard_oldest_bucket():
    global oldest_bucket_timestamp, buckets_queue
    # Discard oldest bucket from the bucket queue
    for bucket in reversed(buckets_queue):
        if len(bucket) > 0:
            bucket.pop()
            break

    oldest_bucket_timestamp = -1

    # Updating oldest bucket timestamp after the discarding previous oldest bucket
    for bucket in reversed(buckets_queue):
        if len(bucket) > 0:
            oldest_bucket_timestamp = bucket[-1]
            break


# DGIM Algorithm Function
# Storing only timestamps for each bucket instead of saving all the bits eg. bucket2(1 ,3), bucket4(5)
# and inserting them in the buckets queue in the order of powers of 2 eg. (1, 2, 4, 8)
def DGIM_algorithm(current_bit):
    global current_timestamp, buckets_queue, oldest_bucket_timestamp
    # Updating current timestamp
    current_timestamp = (current_timestamp + 1) % (reservoir_size * 2)
    # Checking whether to discard oldest bucket and discarding oldest bucket from the bucket queue if TRUE
    discard_bucket_status = discard_oldest_bucket_check(oldest_bucket_timestamp)
    if oldest_bucket_timestamp >= 0 and discard_bucket_status:
        discard_oldest_bucket()
    # Checking whether current element in consideration is 1 or not
    # If it is 0 then do not change bucket queue
    if current_bit is 1:
        # Updating timestamp for current bucket
        latest_timestamp = current_timestamp
        if oldest_bucket_timestamp is -1:
            oldest_bucket_timestamp = current_timestamp
        # Updating corresponding timestamp for each bucket
        for bucket in buckets_queue:
            # Adding timestamp at the first location in the bucket
            bucket.appendleft(latest_timestamp)
            # If number of buckets of same size are less than or equal to 2
            if len(bucket) <= 2:
                break

            # If number of buckets of same size are more than 2 then merging the buckets to a bigger bucket size
            last_timestamp      = bucket.pop()
            latest_timestamp    = bucket.pop()
            # Updating bucket's new timestamp to current timestamp
            if last_timestamp is oldest_bucket_timestamp:
                oldest_bucket_timestamp = latest_timestamp


# Estimating count of 1's using DGIM Algorithm
def estimate_count():
    global buckets_queue
    number_of_1s        = 0
    largest_power_of_2  = 0
    current_power_of_2  = 1
    for bucket in buckets_queue:
        # Number of buckets of same size
        number_of_buckets = len(bucket)
        if number_of_buckets > 0:
            largest_power_of_2  = current_power_of_2
            # Estimated number of ones = A + B, where
            # A. Sum the sizes of all buckets but the last (size means the number of 1s in the bucket)
            # B. Add half the size of the last bucket (largest size bucket)
            number_of_1s        += number_of_buckets * current_power_of_2
        # Changing current power of 2 to higher power
        current_power_of_2 = current_power_of_2 << 1
    return int(number_of_1s - math.floor(largest_power_of_2 / 2))


# Counting actual number of 1's in the window of given size
def actual_count():
    global input_data_window
    number_of_1s = 0
    for current_bit in range(len(input_data_window)):
        # 1 has occurred in the given window
        if input_data_window[current_bit] == 1:
            number_of_1s += 1
    return number_of_1s


# Converting each DStreamRDD into a list and calling DGIM Algorithm for each element of the list
def DStreamRDD_to_list(input_DStreamRDD):
    global input_data_window
    input_bits = input_DStreamRDD.collect()
    # Converting bits from String to Int
    input_bits = map(int, input_bits)
    # Efficient allocation of window of given data stream in the memory
    for element_index in xrange(len(input_bits)):
        # Calling DGIM Algorithm for each new input bit
        DGIM_algorithm(input_bits[element_index])
        # Checking whether window is completely filled with bits
        if len(input_data_window) < reservoir_size:
            input_data_window.appendleft(input_bits[element_index])
        else:
            input_data_window.pop()
            input_data_window.appendleft(input_bits[element_index])

    estimated_ones_count    = estimate_count()
    actual_ones_count       = actual_count()

    if actual_ones_count > 0:
        # Printing result of Estimated and Actual number of 1's in the window of given size
        print "Estimated number of ones in the last 1000 bits:", estimated_ones_count
        print "Actual number of ones in the last 1000 bits:", actual_ones_count
        # print "ERROR:", abs(float(actual_ones_count - estimated_ones_count)) / actual_ones_count * 100, "%"
        print "\n", "\n", "\n"


# Main Function
def main():
    # Creating Streaming Context and batch interval of 10 second
    spark_context = SparkContext("local[2]")
    spark_context.setLogLevel("ERROR")
    spark_streaming_context = StreamingContext(spark_context, 10)

    # Creating a DStream that will connect to localhost:9999
    socket_stream = spark_streaming_context.socketTextStream("localhost", 9999)

    # input_data_stream = socket_stream.window(1000).flatMap(lambda line: line.split(" "))
    # Splitting each line into bits
    input_data_stream = socket_stream.flatMap(lambda line: line.split(" "))

    # Converting each DStreamRDD into a list
    input_data_stream.foreachRDD(DStreamRDD_to_list)

    # Starting the computation
    spark_streaming_context.start()
    # Waiting for the computation to terminate
    spark_streaming_context.awaitTermination()


# Entry point of the program
if __name__ == '__main__':
    main()
