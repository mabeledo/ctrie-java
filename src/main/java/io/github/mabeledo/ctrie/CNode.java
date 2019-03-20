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

import java.util.Arrays;

class CNode<K, V> extends MainNode<K, V> {
    private final int bitmap;
    private final Node<K, V>[] array;
    private final Generation generation;

    CNode(Generation generation) {
        this(0, new Node[0], generation);
    }

    CNode(int bitmap, Node<K, V>[] array, Generation generation) {
        super();
        this.bitmap = bitmap;
        this.array = array;
        this.generation = generation;
    }

    /**
     *
     * @return
     */
    int getBitmap() {
        return this.bitmap;
    }

    /**
     *
     * @param pos
     * @return
     */
    Node<K, V> getChild(int pos) {
        return this.array[pos];
    }

    /**
     *
     * @return
     */
    Generation getGeneration() {
        return this.generation;
    }

    /**
     *
     * @param iNode
     * @param iNodeMain
     * @return
     */
    Node<K, V> resurrect(INode<K, V> iNode, Node<K, V> iNodeMain) {
        if (iNodeMain instanceof TNode) {
            TNode<K, V> tNode = (TNode<K, V>)iNodeMain;
            return new SNode<>(tNode);
        }
        return iNode;
    }

    /**
     *
     * @param level
     * @return
     */
    Node<K, V> contract(int level) {
        if (this.array.length == 1 && level > 0) {
            if (this.array[0] instanceof SNode) {
                SNode<K, V> sNode = (SNode<K, V>)this.array[0];
                return new TNode<>(sNode);
            }
        }
        return this;
    }

    /**
     *
     * @param cTrie
     * @param level
     * @param generation
     * @return
     */
    CNode<K, V> compress(CTrie<K, V> cTrie, int level, Generation generation) {
        int bitmap = this.bitmap;
        @SuppressWarnings("unchecked")
        Node<K, V>[] updatedArray = new Node[this.array.length];

        for (int i = 0; i < this.array.length; i++) {
            Node<K, V> node = this.array[i];
            if (node instanceof INode) {
                INode<K, V> iNode = (INode<K, V>)node;
                Node<K, V> iNodeMain = iNode.genCaSRead(cTrie);
                // TODO: check for null values!
                updatedArray[i] = this.resurrect(iNode, iNodeMain);
            } else if (node instanceof SNode) {
                SNode<K, V> sNode = (SNode<K, V>)node;
                updatedArray[i] = sNode;

            }
        }

        return (CNode<K, V>) new CNode<>(bitmap, updatedArray, generation).contract(level);
    }

    /**
     *
     * @param pos
     * @param node
     * @param generation
     * @return
     */
    CNode<K, V> updateAt(int pos, Node<K, V> node, Generation generation) {
        Node<K, V>[] updatedArray = Arrays.copyOf(this.array, this.array.length);
        updatedArray[pos] = node;
        return new CNode<>(this.bitmap, updatedArray, generation);
    }

    /**
     *
     * @param pos
     * @param node
     * @param generation
     * @return
     */
    CNode<K, V> insertAt(int pos, int flag, Node<K, V> node, Generation generation) {
        int arrayLength = this.array.length;
        @SuppressWarnings("unchecked")
        Node<K, V>[] updatedArray = new Node[arrayLength + 1];
        for (int i = 0; i < arrayLength + 1; i++) {
            if (i != pos) {
                if (i < pos) {
                    updatedArray[i] = this.array[i];
                } else {
                    updatedArray[i] = this.array[i - 1];
                }
            } else {
                updatedArray[i] = node;
            }
        }

        return new CNode<>(this.bitmap | flag, updatedArray, generation);
    }

    /**
     * Returns a copy of this CNode such that all the INodes below it are copied
     * to the specified generation.
     *
     * @param generation
     * @param cTrie
     * @return
     */
    CNode<K, V> renew(Generation generation, CTrie<K, V> cTrie) {
        int currentArrayLength = array.length;
        @SuppressWarnings("unchecked")
        Node<K, V>[] newArray = new Node[currentArrayLength];

        for (int i = 0; i < currentArrayLength; i++) {
            Node<K, V> node = this.array[i];
            if (node instanceof INode) {
                newArray[i] = ((INode<K, V>)node).copyToGeneration(generation, cTrie);
            } else {
                newArray[i] = node;
            }
        }

        return new CNode<>(this.bitmap, newArray, generation);
    }
}
