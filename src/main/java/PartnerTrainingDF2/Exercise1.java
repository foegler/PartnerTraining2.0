/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package PartnerTrainingDF2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KeepTruckin Exercise 1
 * 
 * This is a simple exercise designed to introduce the construction of a
 * Dataflow Pipeline.
 * 
 * This class requires PackageActivityInfo.java
 */
public class Exercise1 {
  private static final Logger LOG = LoggerFactory.getLogger(Exercise1.class);

  public static void main(String[] args) {
    // Create a Pipeline using from any arguments passed in from the
    // Run Configuration.
    Pipeline p =
        Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation()
                                              .create());

    // Create.of generates a PCollection of strings, one per log line,
    // using the small set of log lines contained in the array MINI_LOG.
        p.apply(Create.of(PackageActivityInfo.MINI_LOG))
    // Apply a ParDo using the parsing function provided in
    // PackageActivityInfo.
     .apply(ParDo.of(new PackageActivityInfo.ParseLine()))
     // Define a DoFn inline to log the package info to the console.
     .apply(ParDo.of(new DoFn<PackageActivityInfo, Void>() {
             @ProcessElement
       public void processElement(ProcessContext c) {
         LOG.info(c.element().toString());
       }
     }));

    p.run();
  }
}
