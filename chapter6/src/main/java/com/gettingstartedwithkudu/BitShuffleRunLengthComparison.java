/**
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
package com.gettingstartedwithkudu;

import org.xerial.snappy.BitShuffle;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.xerial.snappy.SnappyLoader;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

public class BitShuffleRunLengthComparison
{
    public static void main( String[] args ) throws Exception {
        testMonotonicallyIncreasing();
        testRandomSmallPositive();
    }
    static void testRandomSmallPositive() throws Exception {
        System.out.println("==== Random Small Positive");
        int[] data = new int[8192];
        Random random = new Random(123456789); // same seed to get predicable results
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt(1000000);
        }
        byte[] dataBytes = toBytes(data);
        int lz4CompressedLength = lz4CompressAndReturnLength(dataBytes);
        int gzipCompressedLength = gzipCompressAndReturnLength(dataBytes);
        int snappyCompressedLength = snappyCompressAndReturnLength(dataBytes);
        int shuffleCompressedLength = lz4CompressAndReturnLength(BitShuffle.bitShuffle(data));
        System.out.println("Plain Size: " + dataBytes.length);
        System.out.println("Plain LZ4 Compressed Size: " + lz4CompressedLength);
        System.out.println("Plain Gzip Compressed Size: " + gzipCompressedLength);
        System.out.println("Plain Snappy Compressed Size: " + snappyCompressedLength);
        System.out.println("BitShuffled LZ4 Compressed Size: " + shuffleCompressedLength);
        RunLength runLength = new RunLength();
        for (int i = 0; i < data.length; i++) {
            runLength.write(data[i]);
        }
        runLength.flush();
        System.out.println("RunLength Encoded LZ4 Compressed Size: " + lz4CompressAndReturnLength(runLength.getArray()));
    }

    static void testMonotonicallyIncreasing() throws Exception {
        System.out.println("==== Monotonically Increasing");
        int[] data = new int[8192];
        for (int i = 0; i < data.length; i++) {
            data[i] = i;
        }
        byte[] dataBytes = toBytes(data);
        int lz4CompressedLength = lz4CompressAndReturnLength(dataBytes);
        int gzipCompressedLength = gzipCompressAndReturnLength(dataBytes);
        int snappyCompressedLength = snappyCompressAndReturnLength(dataBytes);
        int shuffleCompressedLength = lz4CompressAndReturnLength(BitShuffle.bitShuffle(data));
        System.out.println("Plain Size: " + dataBytes.length);
        System.out.println("Plain LZ4 Compressed Size: " + lz4CompressedLength);
        System.out.println("Plain Gzip Compressed Size: " + gzipCompressedLength);
        System.out.println("Plain Snappy Compressed Size: " + snappyCompressedLength);
        System.out.println("BitShuffled LZ4 Compressed Size: " + shuffleCompressedLength);
        RunLength runLength = new RunLength();
        for (int i = 0; i < data.length; i++) {
            runLength.write(data[i]);
        }
        runLength.flush();
        System.out.println("RunLength Encoded LZ4 Compressed Size: " + lz4CompressAndReturnLength(runLength.getArray()));
    }

    static byte[] toBytes(int[] data) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(data.length * 4);
        for (int i = 0; i < data.length; i++) {
            bb.putInt(data[i]);
        }
        return bb.array();
    }
    static int gzipCompressAndReturnLength(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzOut = new GZIPOutputStream(baos);
        gzOut.write(data);
        gzOut.close();
        return baos.size();
    }

    static int snappyCompressAndReturnLength(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SnappyOutputStream sos = new SnappyOutputStream(baos);
        sos.write(data);
        sos.flush();
        return baos.size();
    }

    static int lz4CompressAndReturnLength(byte[] data) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(data.length);
        byte[] compressed = new byte[maxCompressedLength];
        return compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);
    }


    /**
     * Borrowed and then heavily modified from:
     * https://github.com/apache/orc/blob/ca7c97ebb7e5e97a9bc45894d377baa09f637e27/java/core/src/java/org/apache/orc/impl/RunLengthIntegerWriter.java
     */
    static class RunLength{
        static final int MIN_REPEAT_SIZE = 3;
        static final int MAX_DELTA = 127;
        static final int MIN_DELTA = -128;
        static final int MAX_LITERAL_SIZE = 128;
        private static final int MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;
        private final int[] literals = new int[MAX_LITERAL_SIZE];
        private int numLiterals = 0;
        private long delta = 0;
        private boolean repeat = false;
        private int tailRunLength = 0;
        private ByteArrayOutputStream output = new ByteArrayOutputStream();

        public RunLength() {
        }

        private void writeValues() throws IOException {
            if (numLiterals != 0) {
                if (repeat) {
                    output.write(numLiterals - MIN_REPEAT_SIZE);
                    output.write((byte) delta);
                    output.write(new byte[] {
                            (byte)(literals[0] >>> 24),
                            (byte)(literals[0] >>> 16),
                            (byte)(literals[0] >>> 8),
                            (byte)literals[0]});
                } else {
                    output.write(-numLiterals);
                    for(int i=0; i < numLiterals; ++i) {
                        output.write(literals[0]);
                        output.write(new byte[] {
                                (byte)(literals[i] >>> 24),
                                (byte)(literals[i] >>> 16),
                                (byte)(literals[i] >>> 8),
                                (byte)literals[i]});
                    }
                }
                repeat = false;
                numLiterals = 0;
                tailRunLength = 0;
            }
        }

        public void flush() throws IOException {
            writeValues();
            output.flush();
        }

        public byte[] getArray() {
            return output.toByteArray();
        }

        public void write(int value) throws IOException {
            if (numLiterals == 0) {
                literals[numLiterals++] = value;
                tailRunLength = 1;
            } else if (repeat) {
                if (value == literals[0] + delta * numLiterals) {
                    numLiterals += 1;
                    if (numLiterals == MAX_REPEAT_SIZE) {
                        writeValues();
                    }
                } else {
                    writeValues();
                    literals[numLiterals++] = value;
                    tailRunLength = 1;
                }
            } else {
                if (tailRunLength == 1) {
                    delta = value - literals[numLiterals - 1];
                    if (delta < MIN_DELTA || delta > MAX_DELTA) {
                        tailRunLength = 1;
                    } else {
                        tailRunLength = 2;
                    }
                } else if (value == literals[numLiterals - 1] + delta) {
                    tailRunLength += 1;
                } else {
                    delta = value - literals[numLiterals - 1];
                    if (delta < MIN_DELTA || delta > MAX_DELTA) {
                        tailRunLength = 1;
                    } else {
                        tailRunLength = 2;
                    }
                }
                if (tailRunLength == MIN_REPEAT_SIZE) {
                    if (numLiterals + 1 == MIN_REPEAT_SIZE) {
                        repeat = true;
                        numLiterals += 1;
                    } else {
                        numLiterals -= MIN_REPEAT_SIZE - 1;
                        int base = literals[numLiterals];
                        writeValues();
                        literals[0] = base;
                        repeat = true;
                        numLiterals = MIN_REPEAT_SIZE;
                    }
                } else {
                    literals[numLiterals++] = value;
                    if (numLiterals == MAX_LITERAL_SIZE) {
                        writeValues();
                    }
                }
            }
        }

    }
}
