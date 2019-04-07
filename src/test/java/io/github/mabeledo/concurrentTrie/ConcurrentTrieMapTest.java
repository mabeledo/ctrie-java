/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.mabeledo.concurrentTrie;

import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentTrieMapTest {
    @Test
    void putAndGet() {
        ConcurrentTrieMap<String, Long> concurrentTrieMap = new ConcurrentTrieMap<>();
        List<AbstractMap.SimpleEntry<String, Long>> keyValueList =
                IntStream.range(1, 10_000_001)
                        .mapToObj(p ->
                                new AbstractMap.SimpleEntry<>(
                                        "entry-" + ThreadLocalRandom.current().nextInt(p),
                                        ThreadLocalRandom.current().nextLong()))
                        .collect(Collectors.toList());
        Map<String, Long> keyValueMap = keyValueList.stream()
                .collect(Collectors.toMap(
                        AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue, (p, q) -> q));

        keyValueList.forEach(p -> concurrentTrieMap.put(p.getKey(), p.getValue()));

        final long cTrieSize = StreamSupport.stream(concurrentTrieMap.spliterator(), false).count();

        assertEquals(cTrieSize, concurrentTrieMap.size());
        assertEquals(keyValueMap.size(), concurrentTrieMap.size());

        List<Map.Entry<String, Long>> diffFromKeyValueMap =
                keyValueMap.entrySet().stream()
                        .filter(p ->
                                Objects.isNull(concurrentTrieMap.get(p.getKey())) ||
                                        !concurrentTrieMap.get(p.getKey()).equals(p.getValue()))
                        .collect(Collectors.toList());

        assertTrue(diffFromKeyValueMap.isEmpty(), "Offenders: " + diffFromKeyValueMap.toString());

        List<Node<String, Long>> diffFromCTrie =
                concurrentTrieMap.stream()
                        .filter(p ->
                                Objects.isNull(keyValueMap.get(p.getKey())) ||
                                        !keyValueMap.get(p.getKey()).equals(p.getValue()))
                        .collect(Collectors.toList());

        assertTrue(diffFromCTrie.isEmpty(), "Offenders: " + diffFromCTrie.toString());
    }

    @Test
    void loopPutAndGet() {
        final int maxItems = 10_000_001;
        final int iterations = 5;
        ConcurrentTrieMap<String, Long> concurrentTrieMap = new ConcurrentTrieMap<>();

        List<AbstractMap.SimpleEntry<String, Long>> keyValueList =
                IntStream.range(1, maxItems)
                        .mapToObj(p ->
                                new AbstractMap.SimpleEntry<>(
                                        "entry-" + ThreadLocalRandom.current().nextInt(p),
                                        ThreadLocalRandom.current().nextLong()))
                        .collect(Collectors.toList());

        List<Map<String, Long>> keyValueMaps = new ArrayList<>();

        for (int i = 0; i < iterations; i++) {
            Map<String, Long> keyValueMap = keyValueList.stream()
                    .skip(i * (maxItems / iterations))
                    .limit((i + 1) * (maxItems / iterations))
                    .collect(Collectors.toMap(
                            AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue, (p, q) -> q));

            keyValueMaps.add(keyValueMap);
        }

        for (int i = 0; i < iterations; i++) {
            keyValueMaps.get(i).forEach(concurrentTrieMap::put);

            List<Map.Entry<String, Long>> diff =
                    keyValueMaps.get(i).entrySet().stream()
                            .filter(p -> !concurrentTrieMap.get(p.getKey()).equals(p.getValue()))
                            .collect(Collectors.toList());

            assertTrue(diff.isEmpty(), "Offenders: " + diff.toString());
        }

        final Set<String> keySet =
                keyValueList.stream().map(AbstractMap.SimpleEntry::getKey).collect(Collectors.toSet());

        final long cTrieSize = StreamSupport.stream(concurrentTrieMap.spliterator(), false).count();

        assertEquals(cTrieSize, concurrentTrieMap.size());
        assertEquals(keySet.size(), concurrentTrieMap.size());

        List<String> diff =
                keySet.stream()
                        .filter(p -> Objects.isNull(concurrentTrieMap.get(p)))
                        .collect(Collectors.toList());
        assertTrue(diff.isEmpty(), "Offenders: " + diff.toString());
    }

    @Test
    void remove() {
        ConcurrentTrieMap<String, Long> concurrentTrieMap = new ConcurrentTrieMap<>();
        List<Map.Entry<String, Long>> keyValueList =
                IntStream.range(1, 10_000_001)
                        .mapToObj(p ->
                                new AbstractMap.SimpleEntry<>(
                                        "entry-" + ThreadLocalRandom.current().nextInt(p),
                                        ThreadLocalRandom.current().nextLong()))
                        .collect(Collectors.toList());
        Map<String, Long> keyValueMap = keyValueList.stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue, (p, q) -> q));

        keyValueList.forEach(p -> concurrentTrieMap.put(p.getKey(), p.getValue()));

        // Remove some randomly.
        List<Map.Entry<String, Long>> updatedKeyValueList =
                keyValueList.stream()
                        .filter(p -> Math.floorMod(p.getValue(), 2L) == 0)
                        .collect(Collectors.toList());

        updatedKeyValueList.forEach(p -> keyValueMap.remove(p.getKey()));
        updatedKeyValueList.forEach(p -> concurrentTrieMap.remove(p.getKey()));

        final long cTrieSize = StreamSupport.stream(concurrentTrieMap.spliterator(), false).count();

        assertEquals(cTrieSize, concurrentTrieMap.size());
        assertEquals(keyValueMap.size(), concurrentTrieMap.size());

        List<Map.Entry<String, Long>> diffFromKeyValueMap =
                keyValueMap.entrySet().stream()
                        .filter(p ->
                                Objects.isNull(concurrentTrieMap.get(p.getKey())) ||
                                        !concurrentTrieMap.get(p.getKey()).equals(p.getValue()))
                        .collect(Collectors.toList());

        assertTrue(diffFromKeyValueMap.isEmpty(), "Offenders: " + diffFromKeyValueMap.toString());

        List<Node<String, Long>> diffFromCTrie =
                concurrentTrieMap.stream()
                        .filter(p ->
                                Objects.isNull(keyValueMap.get(p.getKey())) ||
                                        !keyValueMap.get(p.getKey()).equals(p.getValue()))
                        .collect(Collectors.toList());

        assertTrue(diffFromCTrie.isEmpty(), "Offenders: " + diffFromCTrie.toString());
    }

    @Test
    void iterator() {

    }

    @Test
    void collisions() {

    }

    @Test
    void snapshots() {

    }
}
