/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_bufistov {

    private static final String FILE = "./m100000000.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);
        System.out.println(Instant.now());

        var inputStream = new BufferedInputStream(new FileInputStream(FILE), 1 << 20);
        Scanner scanner = new Scanner(inputStream);
        long totalSum = 0;
        long linesProcessed = 0;
        int numThreads = 8;
        var pool = new ForkJoinPool(32);
        // var task = pool.submit(() -> Files.lines(Paths.get(FILE)).parallel().map(CalculateAverage_bufistov::lineSum).reduce(0L, Long::sum));
        // System.out.println(task.get());
        byte[] data = Files.readAllBytes(Paths.get(FILE));
        System.out.println(Instant.now() + ": read " + data.length + " bytes");
        System.out.println(Instant.now() + ": totalSum: " + lineSum1(data));
    }

    static long lineSum(String row) {
        long result = 0;
        for (int i = 0; i < row.length(); ++i) {
            result += row.charAt(i);
        }
        return result;
    }

    static long lineSum1(byte[] row) {
        long result = 0;
        for (byte b : row) {
            result += b;
        }
        return result;
    }
}
