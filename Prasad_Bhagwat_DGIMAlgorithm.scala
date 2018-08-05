// Imports required for the program
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


// Global variable declarations
trait global_variables
{
  val reservoir_size		= 1000
  var current_timestamp         = 0
  var oldest_bucket_timestamp   = -1
  var input_data_window         = ListBuffer[Int]()
  var buckets_queue             = ListBuffer.fill(math.ceil(math.log(reservoir_size) / math.log(2)).toInt - 1)(ListBuffer[Int]())
}


object DGIMAlgorithm extends global_variables {
  // Checking whether to discard the oldest bucket or not
  def discard_oldest_bucket_check(last_bucket_timestamp: Int): Boolean ={
    if ((current_timestamp - last_bucket_timestamp) % (reservoir_size * 2) >= reservoir_size)
    {
      true
    }
    else
    {
      false
    }
  }


  // Discarding the oldest bucket from bucket queue
  def discard_oldest_bucket(): Unit ={
    // Discard oldest bucket from the bucket queue
    breakable
    {
      for (bucket <- buckets_queue.reverse)
      {
        if (bucket.nonEmpty)
        {
          val bucket_elements = bucket.dropRight(1)
          bucket.clear()
          bucket_elements ++=: bucket
          break
        }
      }
    }
    oldest_bucket_timestamp = -1

    // Updating oldest bucket timestamp after the discarding previous oldest bucket
    breakable
    {
      for (bucket <- buckets_queue.reverse)
      {
        if (bucket.nonEmpty)
        {
          oldest_bucket_timestamp = bucket.last
          break
        }
      }
    }
  }

  // DGIM Algorithm Function
  // Storing only timestamps for each bucket instead of saving all the bits eg. bucket2(1 ,3), bucket4(5)
  // and inserting them in the buckets queue in the order of powers of 2 eg. (1, 2, 4, 8)
  def DGIM_algorithm(current_bit: Int): Unit = {
    // Updating current timestamp
    current_timestamp = (current_timestamp + 1) % (reservoir_size * 2)
    // Checking whether to discard oldest bucket and discarding oldest bucket from the bucket queue if TRUE
    val discard_bucket_status = discard_oldest_bucket_check(oldest_bucket_timestamp)
    if (oldest_bucket_timestamp >= 0 && discard_bucket_status) {
      discard_oldest_bucket()
    }
    // Checking whether current element in consideration is 1 or not
    // If it is 0 then do not change bucket queue
    if (current_bit == 1)
    {
      // Updating timestamp for current bucket
      var latest_timestamp = current_timestamp
      if (oldest_bucket_timestamp == -1)
      {
        oldest_bucket_timestamp = current_timestamp
      }
      // Updating corresponding timestamp for each bucket
      breakable
      {
        for (bucket <- buckets_queue)
        {
          // Adding timestamp at the first location in the bucket
          latest_timestamp +=: bucket
          // If number of buckets of same size are less than or equal to 2

          if (bucket.size <= 2) {
            break
          }

          // If number of buckets of same size are more than 2 then merging the buckets to a bigger bucket size
          val last_timestamp        = bucket.last
          val intermediate_elements = bucket.dropRight(1)
          bucket.clear()
          latest_timestamp          = intermediate_elements.last
          val bucket_elements       = intermediate_elements.dropRight(1)
          intermediate_elements.clear()
          bucket_elements           ++=: bucket
          // Updating bucket's new timestamp to current timestamp
          if (last_timestamp == oldest_bucket_timestamp)
          {
            oldest_bucket_timestamp = latest_timestamp
          }
        }
      }
    }
  }


  // Estimating count of 1's using DGIM Algorithm
  def estimate_count(): Int ={
    var number_of_1s        = 0
    var largest_power_of_2  = 0
    var current_power_of_2  = 1
    // Number of buckets of same size
    for (bucket <- buckets_queue)
    {
      val number_of_buckets = bucket.size
      if (number_of_buckets > 0)
      {
        largest_power_of_2 = current_power_of_2
        // Estimated number of ones = A + B, where
        // A. Sum the sizes of all buckets but the last
        // (note “size” means the number of 1s in the bucket)
        // B. Add half the size of the last bucket (largest size bucket)
        number_of_1s += number_of_buckets * current_power_of_2
        if(largest_power_of_2 > 256)
        {
          number_of_1s -= largest_power_of_2
        }

        if(largest_power_of_2 == 256 && number_of_buckets == 2)
        {
          number_of_1s -= largest_power_of_2
        }
      }
      // Changing current power of 2 to higher power
      current_power_of_2 = current_power_of_2 << 1
    }
    (number_of_1s - math.floor(largest_power_of_2 / 2)).toInt
  }


  // Counting actual number of 1's in the window of given size
  def actual_count(): Int ={
    var number_of_1s = 0
    for (current_bit <- input_data_window.indices)
    {
      // 1 has occurred in the given window
      if (input_data_window(current_bit) == 1)
      {
        number_of_1s += 1
      }
    }
    number_of_1s
  }

  // Converting each DStreamRDD into a list and calling DGIM Algorithm for each element of the list
  def DStreamRDD_to_list(input_DStreamRDD : RDD[String]): Unit ={
    var input_bits_string = input_DStreamRDD.collect()
    // Converting bits from String to Int
    val input_bits_int    = input_bits_string.map(_.toInt)
    // Efficient allocation of window of given data stream in the memory
    for (element_index <- input_bits_int.indices)
    {
      // Calling DGIM Algorithm for each new input bit
      DGIM_algorithm(input_bits_int(element_index))

      // Checking whether window is completely filled with bits
      if (input_data_window.size < reservoir_size)
      {
        input_bits_int(element_index) +=: input_data_window
      }
      else
      {
        input_data_window = input_data_window.dropRight(1)
        input_bits_int(element_index) +=: input_data_window
      }
    }

    val estimated_ones_count    = estimate_count()
    val actual_ones_count       = actual_count()

    if (actual_ones_count != 0)
    {
      // Printing result of Estimated and Actual number of 1's in the window of given size
      println("Estimated number of ones in the last 1000 bits: " + estimated_ones_count)
      println("Actual number of ones in the last 1000 bits: " + actual_ones_count)
      println("\n\n")
    }
  }


  // Main Function
  def main(args: Array[String]): Unit = {
    // Creating Streaming Context using spark configuration
    val spark_context             = new SparkContext("local[2]", "DGIMAlgorithm")
    spark_context.setLogLevel("ERROR")
    val spark_streaming_context   = new StreamingContext(spark_context, Seconds(10))

    // Creating a DStream that will connect to localhost:9999
    val socket_stream   = spark_streaming_context.socketTextStream("localhost", 9999)
    // Splitting each line into bits
    val input_data_stream = socket_stream.flatMap(line => line.split(" "))

    //Converting each DStreamRDD into a list
    input_data_stream.foreachRDD(input_DStreamRDD => DStreamRDD_to_list(input_DStreamRDD))

    // Starting the computation
    spark_streaming_context.start()
    // Waiting for the computation to terminate
    spark_streaming_context.awaitTermination()
  }
}
