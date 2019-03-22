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

import javax.validation.constraints.NotNull;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class CTrie<K, V> implements Iterable<Node<K, V>> {
    @SuppressWarnings("AtomicFieldUpdaterNotStaticFinal")
    private AtomicReferenceFieldUpdater<CTrie, Object> rootNodeUpdater;
    private volatile Object rootNode;

    private AtomicBoolean readOnly;
    private AtomicInteger size;

    public CTrie() {
        this.rootNodeUpdater = AtomicReferenceFieldUpdater.newUpdater(CTrie.class, Object.class, "rootNode");
        this.rootNode = new INode<>();
        this.readOnly = new AtomicBoolean(false);
        this.size = new AtomicInteger(0);
    }

    private CTrie(Object rootNode, AtomicReferenceFieldUpdater<CTrie, Object> rootNodeUpdater) {
        this.rootNode = rootNode;
        this.rootNodeUpdater = rootNodeUpdater;
    }

    /**
     * @param key
     * @return
     */
    public V get(@NotNull K key) {
        int hashCode = key.hashCode();
        return this.lookupByHashCode(key, hashCode);
    }

    /*
     *
     * @param key
     * @param hashCode
     * @return
     */
    private V lookupByHashCode(@NotNull K key, int hashCode) {
        INode<K, V> root = this.rdcssReadRoot();
        Either<V, Status> result = root.lookup(key, hashCode, 0, null, root.getGeneration(), this);

        if (result.isRight()) {
            Status status = result.right();
            if (status.equals(Status.RESTART)) {
                return this.lookupByHashCode(key, hashCode);
            }
            // Only other possible status here is NOT_FOUND, so null it is.
            return null;
        }

        return result.left();
    }

    /**
     * @param key
     * @param value
     * @return
     */
    public V put(@NotNull K key, V value) {
        int hashCode = key.hashCode();
        return this.insertByHashCode(key, value, hashCode);
    }

    /*
     *
     * @param key
     * @param value
     * @param hashCode
     * @return the value previously associated with the key, or the one that has been just inserted.
     */
    private V insertByHashCode(@NotNull K key, V value, int hashCode) {
        INode<K, V> root = this.rdcssReadRoot();

        Either<V, Status> result;
        do {
            result =
                    root
                            .insert(key, value, hashCode, 0, null, root.getGeneration(), this)
                            .invoke();
        } while (result.isRight() && result.right().equals(Status.RESTART));

        if (result.isLeft()) {
            V previousValue = result.left();
            if (Objects.isNull(previousValue) || !previousValue.equals(value)) {
                this.size.incrementAndGet();
            }
            return Objects.requireNonNullElse(previousValue, value);
        }
        return null;
    }

    /**
     *
     * @param key
     * @return
     */
    public V remove(@NotNull K key) {
        return null;
    }

    /**
     * @return
     */
    public int size() {
        return this.size.get();
    }

    /**
     * @return
     */
    public CTrie<K, V> snapshot() {
        return this.snapshot(true);
    }

    /**
     * @param readOnly
     * @return
     */
    public CTrie<K, V> snapshot(boolean readOnly) {
        return this.recursiveSnapshot(readOnly).invoke();
    }

    /*
     *
     * @param readOnly
     * @return
     */
    @TailRecursive
    private TailCall<CTrie<K, V>> recursiveSnapshot(boolean readOnly) {
        INode<K, V> root = this.rdcssReadRoot();
        MainNode<K, V> rootMainNode = root.genCaSRead(this);

        if (this.rdcssRoot(root, root.copyToGeneration(new Generation(), this), rootMainNode)) {
            if (readOnly) {
                return TailCalls.done(new CTrie<>(root, null));
            }
            return TailCalls.done(new CTrie<>(root, this.rootNodeUpdater));
        }

        return TailCalls.call(() -> this.recursiveSnapshot(readOnly));
    }

    /**
     * @return
     */
    @Override
    public Iterator<Node<K, V>> iterator() {
        if (!this.isReadOnly()) {
            return this.snapshot(true).iterator();
        }

        try {
            return new CTrieIterator<>(this);
        } catch (CTrieIteratorException cti) {
            return CTrieIterator.empty();
        }
    }

    /**
     * @return
     */
    Boolean isReadOnly() {
        return this.readOnly.get();
    }

    // RDCSS methods.
    // From Harris, Fraser, Pratt A practical multi-word compare-and-swap operation.
    // https://timharris.uk/papers/2002-disc.pdf

    /**
     * @return
     */
    INode<K, V> rdcssReadRoot() {
        return this.rdcssReadRoot(false);
    }

    /**
     * @param abort
     * @return
     */
    INode<K, V> rdcssReadRoot(boolean abort) {
        Object potentialRoot = this.rootNodeUpdater.get(this);
        if (potentialRoot instanceof INode) {
            @SuppressWarnings("unchecked")
            INode<K, V> root = (INode<K, V>) potentialRoot;
            return root;
        }

        // In any other case, it has to be a RDCSSDescriptor, then.
        return this.rdcssComplete(abort);
    }

    /**
     * @param oldNode
     * @param newNode
     * @param expectedNode
     * @return
     */
    boolean rdcssRoot(INode<K, V> oldNode, INode<K, V> newNode, MainNode<K, V> expectedNode) {
        RDCSSDescriptor<K, V> descriptor = new RDCSSDescriptor<>(oldNode, newNode, expectedNode);
        if (this.rootNodeUpdater.compareAndSet(this, oldNode, descriptor)) {
            this.rdcssComplete(false);
            return descriptor.isCommitted();
        } else {
            return false;
        }
    }

    /*
     *
     * @param abort
     * @return
     */
    private INode<K, V> rdcssComplete(boolean abort) {
        Object potentialRoot = this.rootNodeUpdater.get(this);

        if (potentialRoot instanceof INode) {
            @SuppressWarnings("unchecked")
            INode<K, V> root = (INode<K, V>) potentialRoot;
            return root;
        } else if (potentialRoot instanceof RDCSSDescriptor) {
            @SuppressWarnings("unchecked")
            RDCSSDescriptor<K, V> descriptor = new RDCSSDescriptor<>((RDCSSDescriptor<K, V>) potentialRoot);

            if (abort) {
                if (this.rootNodeUpdater.compareAndSet(this, descriptor, descriptor.oldNode)) {
                    return descriptor.oldNode;
                }
            } else {
                MainNode<K, V> oldMainNode = descriptor.oldNode.genCaSRead(this);
                if (oldMainNode.equals(descriptor.expectedNode)) {
                    if (this.rootNodeUpdater.compareAndSet(this, potentialRoot, descriptor.newNode)) {
                        descriptor.commited.set(true);
                        return descriptor.newNode;
                    }
                } else {
                    if (this.rootNodeUpdater.compareAndSet(this, potentialRoot, descriptor.oldNode)) {
                        return descriptor.oldNode;
                    }
                }
            }
        }
        return this.rdcssComplete(abort);
    }

    private static class RDCSSDescriptor<K, V> {
        private AtomicBoolean commited;
        private INode<K, V> oldNode;
        private INode<K, V> newNode;
        private MainNode<K, V> expectedNode;

        RDCSSDescriptor(RDCSSDescriptor<K, V> other) {
            this.commited = other.commited;
            this.oldNode = other.oldNode;
            this.newNode = other.newNode;
            this.expectedNode = other.expectedNode;
        }

        RDCSSDescriptor(INode<K, V> oldNode, INode<K, V> newNode, MainNode<K, V> expectedNode) {
            this.commited = new AtomicBoolean(false);
            this.oldNode = oldNode;
            this.newNode = newNode;
            this.expectedNode = expectedNode;
        }

        boolean isCommitted() {
            return this.commited.get();
        }
    }
}
