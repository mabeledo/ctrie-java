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

import javax.validation.constraints.NotNull;

class SNode<K, V> implements Node<K, V> {
    private final K key;
    private final V value;
    private final int hashCode;

    SNode(@NotNull K key, V value, int hashCode) {
        this.key = key;
        this.value = value;
        this.hashCode = hashCode;
    }

    SNode(@NotNull TNode<K, V> tNode) {
        this.key = tNode.getKey();
        this.value = tNode.getValue();
        this.hashCode = tNode.getHashCode();
    }

    K getKey() {
        return this.key;
    }

    V getValue() {
        return this.value;
    }

    int getHashCode() { return this.hashCode; }
}