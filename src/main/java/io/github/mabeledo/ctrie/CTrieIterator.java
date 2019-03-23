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

import io.github.mabeledo.ctrie.exceptions.CTrieIteratorException;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

class CTrieIterator<K, V> implements Iterator<Node<K, V>> {
    private CTrie<K, V> cTrie;
    private Node<K, V>[][] stack;
    private int[] stackPos;
    private int depth;
    private Iterator<Node<K, V>> subIterator;
    private Node<K, V> currentNode;

    @SuppressWarnings("unchecked")
    CTrieIterator(CTrie<K, V> cTrie) throws CTrieIteratorException {
        if (!cTrie.isReadOnly()) {
            throw new CTrieIteratorException("CTrie is not marked as read only!");
        }

        this.cTrie = cTrie;
        this.stack = new Node[7][];
        this.stackPos = new int[7];
        this.depth = -1;
        this.subIterator = null;
        this.currentNode = null;

        this.initialize();
    }

    private CTrieIterator() {
    }

    @Override
    public boolean hasNext() {
        return Objects.nonNull(this.currentNode) || Objects.nonNull(this.subIterator);
    }

    @Override
    public Node<K, V> next() throws NoSuchElementException {
        if (this.hasNext()) {
            Node<K, V> currentNode;
            if (Objects.nonNull(this.subIterator)) {
                Node<K, V> node = this.subIterator.next();
                if (!this.subIterator.hasNext()) {
                    this.subIterator = null;
                    this.advance().invoke();
                }

                currentNode = node;

            } else {
                currentNode = this.currentNode;
                this.advance().invoke();
            }

            return currentNode;
        }

        throw new NoSuchElementException();
    }

    /**
     *
     * @param <K>
     * @param <V>
     * @return
     */
    static <K, V>  CTrieIterator<K, V> empty() {
        return new CTrieIterator<>();
    }

    /*
     *
     */
    private void initialize() {
        this.readINode(this.cTrie.rdcssReadRoot()).invoke();
    }

    /*
     *
     * @param iNode
     */
    @TailRecursive
    private TailCall<Boolean> readINode(INode<K, V> iNode) {
        MainNode<K, V> mainNode = iNode.genCaSRead(this.cTrie);
        if (Objects.isNull(mainNode)) {
            this.currentNode = null;

        } else if (mainNode instanceof TombNode) {
            this.currentNode = mainNode;

        } else if (mainNode instanceof LeafNode) {
            LeafNode<K, V> leafNode = (LeafNode<K, V>) mainNode;
            this.subIterator = leafNode.iterator();

            if (!this.subIterator.hasNext()) {
                this.subIterator = null;
                return this.advance();
            }
            
        } else if (mainNode instanceof CNode) {
            CNode<K, V> cNode = (CNode<K, V>) mainNode;
            this.stack[++this.depth] = cNode.getArray();
            this.stackPos[this.depth] = -1;

            return this.advance();
        }

        return TailCalls.done(true);
    }

    /*
     *
     * @return
     */
    @TailRecursive
    private TailCall<Boolean> advance() {
        if (this.depth >= 0) {
            int pos = this.stackPos[this.depth] + 1;
            if (pos < this.stack[this.depth].length) {
                this.stackPos[this.depth] = pos;

                Node<K, V> node = this.stack[this.depth][pos];
                if (node instanceof SingletonNode) {
                    this.currentNode = node;

                } else if (node instanceof INode) {
                    return this.readINode((INode<K, V>) node);
                }

            } else {
                this.depth--;

                return this.advance();
            }
        } else {
            this.currentNode = null;
        }

        return TailCalls.done(true);
    }
}
