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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Leaf node.
 *
 * @param <K>
 * @param <V>
 */
class LeafNode<K, V> extends MainNode<K, V> {
    private final Map<K, V> collisionMap;

    LeafNode(K firstKey, V firstValue, K secondKey, V secondValue) {
        super();
        this.collisionMap = Map.of(firstKey, firstValue, secondKey, secondValue);
    }

    private LeafNode(Map<K, V> collisionMap) {
        super();
        this.collisionMap = collisionMap;
    }

    /**
     * @param key
     * @return
     */
    Either<V, Status> get(K key) {
        return this.collisionMap.containsKey(key) ?
                Either.left(this.collisionMap.get(key)) :
                Either.right(Status.NOT_FOUND);
    }

    /**
     * @param key
     * @param value
     * @return
     */
    LeafNode<K, V> insert(K key, V value) {
        Map<K, V> updatedMap =
                Stream.concat(this.collisionMap.entrySet().stream(), Map.of(key, value).entrySet().stream())
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (p, q) -> q));
        return new LeafNode<>(updatedMap);
    }

    /**
     *
     * @param key
     * @return
     */
    MainNode<K, V> remove(K key) {
        Map<K, V> updatedMap =
                this.collisionMap.entrySet().stream()
                        .filter(p -> !p.getKey().equals(key))
                        .collect(Collectors.toUnmodifiableMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        ));

        if (updatedMap.size() > 1) {
            return new LeafNode<>(updatedMap);
        } else {
            // Only one element, so it is going to be a TombNode and get compressed afterwards.
            // Here the assumption is that it *has* to have at least one element, as there is no way a leaf can be
            // created with less than two initial elements.
            Map.Entry<K, V> entry = updatedMap.entrySet().iterator().next();
            return new TombNode<>(entry.getKey(), entry.getValue(), entry.getKey().hashCode());
        }
    }

    /**
     * @return
     */
    Iterator<Node<K, V>> iterator() {
        return
                this.collisionMap.entrySet().stream()
                        .map(p -> (Node<K, V>) new TombNode<>(p.getKey(), p.getValue(), p.getKey().hashCode()))
                        .iterator();
    }
}
