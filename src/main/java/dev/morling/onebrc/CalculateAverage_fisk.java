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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;

public class CalculateAverage_fisk {
    private static final String FILE = "./measurements.txt";

    private static long findSegmentBoundary(RandomAccessFile raf, int i, int skipForSegment, long location, long fileSize) throws IOException {
        if (i == skipForSegment)
            return location;

        raf.seek(location);
        while (location < fileSize) {
            location++;
            if (raf.read() == '\n')
                break;
        }
        return location;
    }

    static class RadixTreeNode {
        public long sum = 0;
        public int min = Integer.MAX_VALUE;
        public int max = Integer.MIN_VALUE;
        public int count = 0;

        public int keyLength;

        byte[] prefix;
        RadixTreeNode[] children = new RadixTreeNode[256];
    }

    private static RadixTreeNode insertRadixTree(MemorySegment memory, int pos, RadixTreeNode node) {
        int lookahead = 0;
        byte b;
        int origPos = pos;
        while ((b = memory.get(ValueLayout.JAVA_BYTE, pos++)) != ';') {
            if (lookahead > 0 && lookahead < node.prefix.length) {
                if (node.prefix[lookahead] == b) {
                    // Consume characters as long as lookahead matches prefix
                    lookahead++;
                }
                else {
                    // Not matching prefix; clamp prefix and split node
                    byte lab = node.prefix[lookahead];
                    var firstPrefix = Arrays.copyOf(node.prefix, lookahead);
                    var secondPrefix = Arrays.copyOfRange(node.prefix, lookahead, node.prefix.length);

                    var children = node.children;

                    // Readjust children after prefix clamping
                    var child = new RadixTreeNode();
                    child.prefix = secondPrefix;

                    node.children = child.children;
                    child.children = children;
                    child.max = node.max;
                    child.min = node.min;
                    child.count = node.count;
                    child.sum = node.sum;

                    node.prefix = firstPrefix;
                    node.max = Integer.MIN_VALUE;
                    node.min = Integer.MAX_VALUE;
                    node.count = 0;
                    node.sum = 0;

                    node.children[((int) lab) & 0xFF] = child;

                    lookahead = 1;
                    int prefix = ((int) b) & 0xFF;
                    // Other split node where the prefix split
                    child = node.children[prefix] = new RadixTreeNode();
                    child.prefix = new byte[]{ b };
                    while ((b = memory.get(ValueLayout.JAVA_BYTE, pos++)) != ';') {
                        child.prefix = Arrays.copyOf(child.prefix, child.prefix.length + 1);
                        child.prefix[child.prefix.length - 1] = b;
                    }
                    node = child;
                    break;
                }
                continue;
            }

            lookahead = 1;
            int prefix = ((int) b) & 0xFF;
            var child = node.children[prefix];
            if (child == null) {
                // Opportunistically dump entire prefix in new node
                child = node.children[prefix] = new RadixTreeNode();
                child.prefix = new byte[]{ b };
                while ((b = memory.get(ValueLayout.JAVA_BYTE, pos++)) != ';') {
                    child.prefix = Arrays.copyOf(child.prefix, child.prefix.length + 1);
                    child.prefix[child.prefix.length - 1] = b;
                }

                node = child;
                break;
            }
            node = child;
        }

        node.keyLength = pos - origPos;

        return node;
    }

    private static RadixTreeNode lookupRadixTree(MemorySegment memory, int pos, RadixTreeNode node) {
        int origPos = pos;

        // First character can't be ; because that would be an empty name
        byte b = memory.get(ValueLayout.JAVA_BYTE, pos++);
        int prefix = ((int) b) & 0xFF;
        node = node.children[prefix];
        if (node == null) {
            return null;
        }

        int lookahead = 1;

        while ((b = memory.get(ValueLayout.JAVA_BYTE, pos++)) != ';') {
            if (lookahead < node.prefix.length) {
                if (node.prefix[lookahead++] != b) {
                    // Not matching prefix; requires mutation
                    node = null;
                    break;
                }
            }
            else {
                lookahead = 1;
                prefix = ((int) b) & 0xFF;
                node = node.children[prefix];
                if (node == null) {
                    // Unknown node; requires mutation
                    break;
                }
            }
        }

        if (node != null && node.keyLength == 0) {
            node.keyLength = pos - origPos;
        }

        return node;
    }

    public static void main(String[] args) throws Exception {
        var path = Path.of(FILE);
        var start = Instant.now();
        var desiredSegmentsCount = Runtime.getRuntime().availableProcessors() * 10;

        var threads = new ArrayList<Thread>();
        var threadRoots = new RadixTreeNode[desiredSegmentsCount];

        try (var raf = new RandomAccessFile(path.toFile(), "r")) {
            var fileSize = raf.length();
            var segmentSize = fileSize / desiredSegmentsCount;
            for (int segmentIdx = 0; segmentIdx < desiredSegmentsCount; segmentIdx++) {
                var segStart = segmentIdx * segmentSize;
                var segEnd = (segmentIdx == desiredSegmentsCount - 1) ? fileSize : segStart + segmentSize;
                segStart = findSegmentBoundary(raf, segmentIdx, 0, segStart, segEnd);
                segEnd = findSegmentBoundary(raf, segmentIdx, desiredSegmentsCount - 1, segEnd, fileSize);

                var segSize = segEnd - segStart;

                var segStartFinal = segStart;
                var segmentIdxFinal = segmentIdx;

                var thread = Thread.ofVirtual().start(() -> {
                    try (var fileChannel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ)) {
                        var memory = fileChannel.map(FileChannel.MapMode.READ_ONLY, segStartFinal, segSize, Arena.ofConfined());
                        var root = threadRoots[segmentIdxFinal] = new RadixTreeNode();
                        int position = 0;
                        long limit = memory.byteSize();
                        while (position < limit) {
                            position = processLine(memory, position, root);
                        }
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                threads.add(thread);
            }

            for (var thread : threads) {
                thread.join();
            }
        }

        summarize(threadRoots);

        if (start != null)
            System.err.println(Duration.between(start, Instant.now()));
    }

    private static int processLine(MemorySegment memory, int position, RadixTreeNode root) {
        RadixTreeNode node = lookupRadixTree(memory, position, root);

        if (node == null) {
            // Slow path; we need to mutate the radix tree
            node = insertRadixTree(memory, position, root);
        }

        int pos = position + node.keyLength;

        int temperature;
        int negative = 1;
        // Unrolled parsing
        if (memory.get(ValueLayout.JAVA_BYTE, pos) == '-') {
            negative = -1;
            pos++;
        }
        if (memory.get(ValueLayout.JAVA_BYTE, pos + 1) == '.') {
            temperature = negative * ((memory.get(ValueLayout.JAVA_BYTE, pos) - '0') * 10 +
                    (memory.get(ValueLayout.JAVA_BYTE, pos + 2) - '0'));
            pos += 3;
        }
        else {
            temperature = negative * ((memory.get(ValueLayout.JAVA_BYTE, pos) - '0') * 100 +
                    ((memory.get(ValueLayout.JAVA_BYTE, pos + 1) - '0') * 10 +
                            (memory.get(ValueLayout.JAVA_BYTE, pos + 3) - '0')));
            pos += 4;
        }
        if (memory.get(ValueLayout.JAVA_BYTE, pos) == '\r') {
            pos++;
        }
        pos++;

        node.sum += temperature;
        node.count++;

        if (temperature < node.min) {
            node.min = temperature;
        }
        if (temperature > node.max) {
            node.max = temperature;
        }

        return pos;
    }

    private static void summarize(RadixTreeNode[] threadRoots) {
        int segments = threadRoots.length;

        var current = new RadixTreeNode[segments];
        for (RadixTreeNode root : threadRoots) {

        }
        /*
         * var result = new TreeMap<String, String>();
         * for (var i = 0; i < HASH_NO_CLASH_MODULUS; i++) {
         * String name = null;
         * 
         * var min = Integer.MAX_VALUE;
         * var max = Integer.MIN_VALUE;
         * var sum = 0L;
         * var count = 0L;
         * for (Tracker tracker : trackers) {
         * if (tracker.names[i] == null)
         * continue;
         * if (name == null)
         * name = tracker.names[i];
         * 
         * var minn = tracker.minMaxCount[i * 3];
         * var maxx = tracker.minMaxCount[i * 3 + 1];
         * if (minn < min)
         * min = minn;
         * if (maxx > max)
         * max = maxx;
         * count += tracker.minMaxCount[i * 3 + 2];
         * sum += tracker.sums[i];
         * }
         * if (name == null)
         * continue;
         * 
         * var mean = Math.round((double) sum / count) / 10.0;
         * result.put(name, (min / 10.0) + "/" + mean + "/" + (max / 10.0));
         * }
         */
    }
}
