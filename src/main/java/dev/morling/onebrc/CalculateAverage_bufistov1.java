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
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateAverage_bufistov1 {

    static final long LINE_SEPARATOR = '\n';
    // static final Threadlong newLines = 0;

    public static class FileRead implements Callable<Long> {

        private FileChannel _channel;
        private long _startLocation;
        private int _size;

        private long newLines = 0;

        public FileRead(long loc, int size, FileChannel chnl) {
            _startLocation = loc;
            _size = size;
            _channel = chnl;
        }

        @Override
        public Long call() throws IOException {
            try {
                log("Reading the channel: " + _startLocation + ":" + _size);
                byte[] suffix = new byte[128];
                if (_startLocation > 0) {
                    toLineBegin(suffix);
                }
                int bufferSize = 1 << 24;
                while (_size > 0) {
                    if (_size < bufferSize) {
                        bufferSize = _size;
                    }
                    MappedByteBuffer byteBuffer = _channel.map(FileChannel.MapMode.READ_ONLY, _startLocation, bufferSize);
                    _size -= bufferSize;
                    _startLocation += bufferSize;
                    int suffixBytes = 0;
                    if (_size > 0) {
                        suffixBytes = toLineBegin(suffix);
                    }
                    countLines(byteBuffer, suffix, suffixBytes);
                }
                log("Done Reading the channel: " + _startLocation + ":" + _size);
                return newLines;
            }
            catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        byte getByte(long position) throws IOException {
            MappedByteBuffer byteBuffer = _channel.map(FileChannel.MapMode.READ_ONLY, position, 1);
            return byteBuffer.get();
        }

        int toLineBegin(byte[] suffix) throws IOException {
            int bytesConsumed = 0;
            if (getByte(_startLocation - 1) != LINE_SEPARATOR) {
                while (getByte(_startLocation) != LINE_SEPARATOR) {
                    suffix[bytesConsumed++] = getByte(_startLocation);
                    ++_startLocation;
                    --_size;
                }
            }
            if (bytesConsumed > 0) {
                ++_startLocation;
                --_size;
                ++newLines;
            }
            return bytesConsumed;
        }

        long countLines(MappedByteBuffer byteBuffer, byte[] suffix, int suffixBytes) {
            byte nextByte;
            long result = 0;
            while (byteBuffer.hasRemaining()) {
                nextByte = byteBuffer.get();
                result += nextByte;
                if (nextByte == LINE_SEPARATOR) {
                    newLines += 1L;
                }
            }
            return result;
        }
    }

    public static void main(String[] args) throws Exception {
        FileInputStream fileInputStream = new FileInputStream(args[0]);
        int numThreads = Integer.parseInt(args[1]);
        log("File: " + args[0]);
        log("numThreads: " + numThreads);
        FileChannel channel = fileInputStream.getChannel();
        final long fileSize = channel.size();
        long remaining_size = fileSize;
        long chunk_size = Math.min(fileSize / numThreads, Integer.MAX_VALUE - 5);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        long start_loc = 0;
        ArrayList<Future<Long>> results = new ArrayList<>(numThreads);
        while (remaining_size >= chunk_size) {
            results.add(executor.submit(new FileRead(start_loc, toIntExact(chunk_size), channel)));
            remaining_size = remaining_size - chunk_size;
            start_loc = start_loc + chunk_size;
        }

        // load the last remaining piece
        results.add(executor.submit(new FileRead(start_loc, toIntExact(remaining_size), channel)));
        executor.shutdown();

        // Wait for all threads to finish
        while (!executor.isTerminated()) {
            Thread.yield();
        }
        log("Finished all threads");
        fileInputStream.close();
        long result = 0;
        for (var future : results) {
            assert future.isDone();
            result += future.get();
        }
        log("Result: " + result);
        // log("Total lines: " + newLines);
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

    static long lineSum2(MappedByteBuffer byteBuffer) {
        long result = 0;
        while (byteBuffer.hasRemaining()) {
            result += byteBuffer.get();
        }
        return result;
    }

    static void log(String message) {
        System.out.println(Instant.now() + "[" + Thread.currentThread().getName() + "]: " + message);
    }
}
