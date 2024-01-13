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

import static java.lang.Math.toIntExact;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Future;

class ResultRow {
    byte[] station;

    String stationString;
    long min, max, count, suma;

    ResultRow(byte[] station, long value) {
        this.station = new byte[station.length];
        System.arraycopy(station, 0, this.station, 0, station.length);
        this.min = value;
        this.max = value;
        this.count = 1;
        this.suma = value;
    }

    public String toString() {
        stationString = new String(station, StandardCharsets.UTF_8);
        return stationString + "=" + round(min / 10.0) + "/" + round(suma / 10.0 / count) + "/" + round(max / 10.0);
    }

    private double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    ResultRow update(long newValue) {
        this.count += 1;
        this.suma += newValue;
        if (newValue < this.min) {
            this.min = newValue;
        }
        else if (newValue > this.max) {
            this.max = newValue;
        }
        return this;
    }

    ResultRow merge(ResultRow another) {
        this.count += another.count;
        this.suma += another.suma;
        this.min = Math.min(this.min, another.min);
        this.max = Math.max(this.max, another.max);
        return this;
    }
}

class ByteArrayWrapper {
    private final byte[] data;

    public ByteArrayWrapper(byte[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object other) {
        return Arrays.equals(data, ((ByteArrayWrapper) other).data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}

public class CalculateAverage_bufistov1 {

    static final long LINE_SEPARATOR = '\n';

    public static class FileRead implements Callable<HashMap<ByteArrayWrapper, ResultRow>> {

        private final FileChannel fileChannel;
        private long currentLocation;
        private int bytesToRead;

        public FileRead(long startLocation, int bytesToRead, FileChannel fileChannel) {
            this.currentLocation = startLocation;
            this.bytesToRead = bytesToRead;
            this.fileChannel = fileChannel;
        }

        @Override
        public HashMap<ByteArrayWrapper, ResultRow> call() throws IOException {
            try {
                HashMap<ByteArrayWrapper, ResultRow> result = new HashMap<>(10000);
                log("Reading the channel: " + currentLocation + ":" + bytesToRead);
                byte[] suffix = new byte[128];
                if (currentLocation > 0) {
                    toLineBegin(suffix);
                }
                while (bytesToRead > 0) {
                    int bufferSize = Math.min(1 << 24, bytesToRead);
                    MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, currentLocation, bufferSize);
                    bytesToRead -= bufferSize;
                    currentLocation += bufferSize;
                    int suffixBytes = 0;
                    if (currentLocation < fileChannel.size()) {
                        suffixBytes = toLineBegin(suffix);
                    }
                    processChunk(byteBuffer, bufferSize, suffix, suffixBytes, result);
                }
                log("Done Reading the channel: " + currentLocation + ":" + bytesToRead);
                return result;
            }
            catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        byte getByte(long position) throws IOException {
            MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, 1);
            return byteBuffer.get();
        }

        int toLineBegin(byte[] suffix) throws IOException {
            int bytesConsumed = 0;
            if (getByte(currentLocation - 1) != LINE_SEPARATOR) {
                while (getByte(currentLocation) != LINE_SEPARATOR) { // Small bug here if last chunk is less than a line and has now '\n' at the end
                    suffix[bytesConsumed++] = getByte(currentLocation);
                    ++currentLocation;
                    --bytesToRead;
                }
                ++currentLocation;
                --bytesToRead;
            }
            return bytesConsumed;
        }

        void processChunk(MappedByteBuffer byteBuffer, int bufferSize, byte[] suffix, int suffixBytes, HashMap<ByteArrayWrapper, ResultRow> result) {
            int nameBegin = 0;
            int numberBegin = -1;
            byte[] stationName = null;

            for (int currentPosition = 0; currentPosition < bufferSize; ++currentPosition) {
                byte nextByte = byteBuffer.get(currentPosition);
                if (nextByte == ';') {
                    stationName = new byte[currentPosition - nameBegin];
                    byteBuffer.slice(nameBegin, stationName.length).get(stationName, 0, stationName.length);
                    numberBegin = currentPosition + 1;
                }
                else if (nextByte == LINE_SEPARATOR) {
                    long value = getValue(byteBuffer, numberBegin, currentPosition);
                    // log("Station name: '" + new String(stationName, StandardCharsets.UTF_8) + "' value: " + value);
                    ResultRow row = new ResultRow(stationName, value);
                    var byteKey = new ByteArrayWrapper(stationName);
                    result.merge(byteKey, row, (old, x) -> old.update(x.suma));
                    nameBegin = currentPosition + 1;
                    stationName = null;
                }
            }
            if (nameBegin < bufferSize) {
                byte[] lastLine = new byte[bufferSize - nameBegin + suffixBytes];
                byte[] prefix = new byte[bufferSize - nameBegin];
                byteBuffer.slice(nameBegin, prefix.length).get(prefix, 0, prefix.length);
                System.arraycopy(prefix, 0, lastLine, 0, prefix.length);
                System.arraycopy(suffix, 0, lastLine, prefix.length, suffixBytes);
                processLastLine(lastLine, result);
            }
        }

        void processLastLine(byte[] lastLine, HashMap<ByteArrayWrapper, ResultRow> result) {
            int numberBegin = -1;
            byte[] stationName = null;
            for (int i = 0; i < lastLine.length; ++i) {
                byte nextByte = lastLine[i];
                if (nextByte == ';') {
                    stationName = new byte[i];
                    System.arraycopy(lastLine, 0, stationName, 0, stationName.length);
                    numberBegin = i + 1;
                }
            }
            long value = getValue(lastLine, numberBegin);
            ResultRow row = new ResultRow(stationName, value);
            var byteKey = new ByteArrayWrapper(stationName);
            result.merge(byteKey, row, (current, x) -> current.update(x.suma));
        }

        long getValue(MappedByteBuffer byteBuffer, int startLocation, int endLocation) {
            byte nextByte = byteBuffer.get(startLocation);
            boolean negate = nextByte == '-';
            long result = negate ? 0 : nextByte - '0';
            for (int i = startLocation + 1; i < endLocation; ++i) {
                nextByte = byteBuffer.get(i);
                if (nextByte != '.') {
                    result *= 10;
                    result += nextByte - '0';
                }
            }
            return negate ? -result : result;
        }

        long getValue(byte[] lastLine, int startLocation) {
            byte nextByte = lastLine[startLocation];
            boolean negate = nextByte == '-';
            long result = negate ? 0 : nextByte - '0';
            for (int i = startLocation + 1; i < lastLine.length; ++i) {
                nextByte = lastLine[i];
                if (nextByte != '.') {
                    result *= 10;
                    result += nextByte - '0';
                }
            }
            return negate ? -result : result;
        }
    }

    public static void main(String[] args) throws Exception {
        String fileName = "measurements.txt";
        if (args.length > 0 && args[0].length() > 0) {
            fileName = args[0];
        }
        log("InputFile: " + fileName);
        FileInputStream fileInputStream = new FileInputStream(fileName);
        int numThreads = 8;
        if (args.length > 1) {
            numThreads = Integer.parseInt(args[1]);
        }
        log("NumThreads: " + numThreads);
        FileChannel channel = fileInputStream.getChannel();
        final long fileSize = channel.size();
        long remaining_size = fileSize;
        long chunk_size = Math.min((fileSize + numThreads - 1) / numThreads, Integer.MAX_VALUE - 5);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        long startLocation = 0;
        ArrayList<Future<HashMap<ByteArrayWrapper, ResultRow>>> results = new ArrayList<>(numThreads);
        while (remaining_size > 0) {
            long actualSize = Math.min(chunk_size, remaining_size);
            results.add(executor.submit(new FileRead(startLocation, toIntExact(actualSize), channel)));
            remaining_size -= actualSize;
            startLocation += actualSize;
        }
        executor.shutdown();

        // Wait for all threads to finish
        while (!executor.isTerminated()) {
            Thread.yield();
        }
        log("Finished all threads");
        fileInputStream.close();
        HashMap<ByteArrayWrapper, ResultRow> result = new HashMap<>(20000);
        for (var future : results) {
            for (var entry : future.get().entrySet()) {
                result.merge(entry.getKey(), entry.getValue(), ResultRow::merge);
            }
        }
        ResultRow[] finalResult = result.values().toArray(new ResultRow[0]);
        for (var row : finalResult) {
            row.toString();
        }
        Arrays.sort(finalResult, Comparator.comparing(a -> a.stationString));
        System.out.println("{" + String.join(", ", Arrays.stream(finalResult).map(ResultRow::toString).toList()) + "}");
        log("All done!");
    }

    static void log(String message) {
        // System.err.println(Instant.now() + "[" + Thread.currentThread().getName() + "]: " + message);
    }
}
