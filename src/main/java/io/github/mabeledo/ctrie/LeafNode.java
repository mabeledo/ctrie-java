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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Leaf node.
 *
 * @param <K>
 * @param <V>
 */
class LeafNode<K, V> extends MainNode<K, V> {
    private final Map<K, V> collisionMap;

    private LeafNode() {
        super();
        this.collisionMap = new LinkedHashMap<>();
    }

    LeafNode(K key, V value) {
        this();
        this.collisionMap.put(key, value);
    }

    LeafNode(K firstKey, V firstValue, K secondKey, V secondValue) {
        this();
        this.collisionMap.put(firstKey, firstValue);
        this.collisionMap.put(secondKey, secondValue);
    }

    LeafNode(Map<K, V> collisionMap, K key, V value) {
        super();
        this.collisionMap = collisionMap;
        this.collisionMap.put(key, value);
    }

    /**
     *
     * @return
     */
    Map<K, V> getCollisionMap() {
        return this.collisionMap;
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
        return new LeafNode<>(this.collisionMap, key, value);
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
