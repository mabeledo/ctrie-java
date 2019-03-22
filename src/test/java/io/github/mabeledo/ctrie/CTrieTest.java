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

package io.github.mabeledo.ctrie;

import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CTrieTest {
    @Test
    void putAndGet() {
        // Simply put and get some random values.
        CTrie<String, Long> cTrie = new CTrie<>();
        List<AbstractMap.SimpleEntry<String, Long>> keyValueList =
                IntStream.range(1, 1_000_001)
                        .mapToObj(p -> new AbstractMap.SimpleEntry<>("entry-" + ThreadLocalRandom.current().nextInt(p), ThreadLocalRandom.current().nextLong()))
                        .collect(Collectors.toList());
        Map<String, Long> keyValueMap = keyValueList.stream()
                .collect(Collectors.toMap(
                        AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue, (p, q) -> q));

        keyValueList.forEach(p -> cTrie.put(p.getKey(), p.getValue()));

        assertEquals(keyValueMap.size(), cTrie.size());

        List<Map.Entry<String, Long>> diff =
                keyValueMap.entrySet().stream()
                        .filter(p -> !cTrie.get(p.getKey()).equals(p.getValue()))
                        .collect(Collectors.toList());

        assertTrue(diff.isEmpty(), "Offenders: " + diff.toString());
    }

    @Test
    void collisions() {

    }

    @Test
    void snapshots() {

    }

    @Test
    void rdcssReadRoot() {

    }
}
