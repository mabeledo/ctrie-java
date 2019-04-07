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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

abstract class MainNode<K, V> implements Node<K, V> {
    private static final AtomicReferenceFieldUpdater<MainNode, MainNode> PREVIOUS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(MainNode.class, MainNode.class, "previous");
    private volatile MainNode<K, V> previous;

    MainNode() {
        this.previous = null;
    }

    /**
     * @param oldValue
     * @param newValue
     * @return
     */
    boolean casPrevious(MainNode<K, V> oldValue, MainNode<K, V> newValue) {
        return MainNode.PREVIOUS_UPDATER.compareAndSet(this, oldValue, newValue);
    }

    /**
     * @param newValue
     */
    void writePrevious(MainNode<K, V> newValue) {
        MainNode.PREVIOUS_UPDATER.set(this, newValue);
    }

    /**
     * @return
     */
    MainNode<K, V> readPrevious() {
        @SuppressWarnings("unchecked")
        MainNode<K, V> result = (MainNode<K, V>) MainNode.PREVIOUS_UPDATER.get(this);
        return result;
    }

    /**
     * @param leftNode
     * @param leftHashCode
     * @param rightNode
     * @param rightHashCode
     * @param level
     * @param generation
     * @param <K>
     * @param <V>
     * @return
     */
    static <K, V> MainNode<K, V> dual(SingletonNode<K, V> leftNode, int leftHashCode, SingletonNode<K, V> rightNode, int rightHashCode, int level, Generation generation) {
        if (level < 35) {
            int leftIndex = (leftHashCode >>> level) & 0x1f;
            int rightIndex = (rightHashCode >>> level) & 0x1f;
            int bitmap = (1 << leftIndex) | (1 << rightIndex);

            if (leftIndex == rightIndex) {
                IndirectionNode<K, V> subIndirectionNode =
                        new IndirectionNode<>(
                                dual(leftNode, leftHashCode, rightNode, rightHashCode, level + 5, generation),
                                generation);
                @SuppressWarnings("unchecked")
                CNode<K, V> cNode = new CNode<K, V>(bitmap, new Node[]{subIndirectionNode}, generation);
                return cNode;
            } else {
                if (leftIndex < rightIndex) {
                    @SuppressWarnings("unchecked")
                    CNode<K, V> cNode = new CNode<K, V>(bitmap, new Node[]{leftNode, rightNode}, generation);
                    return cNode;
                } else {
                    @SuppressWarnings("unchecked")
                    CNode<K, V> cNode = new CNode<K, V>(bitmap, new Node[]{rightNode, leftNode}, generation);
                    return cNode;
                }
            }
        } else {
            return new LeafNode<>(leftNode.getKey(), leftNode.getValue(), rightNode.getKey(), rightNode.getValue());
        }
    }
}
