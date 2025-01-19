package com.streamwork.ch03.job;

import com.streamwork.ch03.api.*;
import com.streamwork.ch03.engine.JobStarter;

public class ParallelizedVehicleCountJob3 {

  public static void main(String[] args) {
    Job job = new Job("parallelized_vehicle_count");

//    Stream bridgeStream = job.addSource(new SensorReader("sensor-reader", 2, 9990));
    Stream bridgeStream = job.addSource(new ContinuousVehicleSource("Continuous-reader", 2, 1000));
//    bridgeStream.applyOperator(new VehicleCounter("vehicle-counter", 2, new FieldsGrouping()));

    Stream mapStream = bridgeStream.applyOperator(new VehicleMapper("vehicle-Mapper", 2));

    mapStream.applyOperator(new VehicleCounter("vehicle-counter", 2));

    System.out.println("This is a streaming job that counts vehicles from the input stream " +
            "in real time. Please enter vehicle types like 'car' and 'truck' in the " +
            "input terminals and look at the output");
    JobStarter starter = new JobStarter(job);
    starter.start();
  }
}
