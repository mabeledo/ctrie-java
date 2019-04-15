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

import io.github.mabeledo.concurrentTrie.exceptions.IteratorException;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ConcurrentTrieMap<K, V> implements Map<K, V>, Iterable<Node<K, V>> {
    private static final AtomicReferenceFieldUpdater<ConcurrentTrieMap, Object> ROOT_NODE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ConcurrentTrieMap.class, Object.class, "rootNode");
    private volatile Object rootNode;

    private AtomicBoolean readOnly;

    public ConcurrentTrieMap() {
        this.rootNode = new IndirectionNode<>();
        this.readOnly = new AtomicBoolean(false);
    }

    private ConcurrentTrieMap(Object rootNode, boolean readOnly) {
        this.rootNode = rootNode;
        this.readOnly = new AtomicBoolean(readOnly);
    }

    /*
     * Read methods.
     */

    /**
     * @param key
     * @return
     */
    @Override
    public V get(Object key) throws NullPointerException {
        if (Objects.isNull(key)) {
            throw new NullPointerException();
        }

        @SuppressWarnings("unchecked")
        K castedKey = (K) key;
        int hashCode = key.hashCode();

        Either<V, Status> result = this.lookupByHashCode(castedKey, hashCode);
        return result.isLeft() ? result.left() : null;
    }

    /**
     * @param key
     * @return
     * @throws NullPointerException
     */
    @Override
    public boolean containsKey(Object key) throws NullPointerException {
        Objects.requireNonNull(key);

        @SuppressWarnings("unchecked")
        K castedKey = (K) key;
        int hashCode = key.hashCode();

        Either<V, Status> result = this.lookupByHashCode(castedKey, hashCode);
        return result.isLeft();
    }

    /**
     * @param value
     * @return
     */
    @Override
    public boolean containsValue(Object value) {
        Objects.requireNonNull(value);

        return this.stream()
                .map(Node::getValue)
                .anyMatch(p -> Objects.equals(value, p));
    }

    /*
     *
     * @param key
     * @param hashCode
     * @return
     */
    private Either<V, Status> lookupByHashCode(K key, int hashCode) throws NullPointerException {
        Objects.requireNonNull(key);

        IndirectionNode<K, V> root = this.rdcssReadRoot();
        Either<V, Status> result = root.lookup(key, hashCode, 0, null, root.getGeneration(), this);

        if (result.isRight()) {
            Status status = result.right();
            if (status.equals(Status.RESTART)) {
                return this.lookupByHashCode(key, hashCode);
            }
        }

        return result;
    }

    /*
     * Write methods.
     */

    /**
     * @param key
     * @param value
     * @return
     */
    @Override
    public V put(K key, V value) throws NullPointerException {
        Objects.requireNonNull(key);

        Either<V, Status> result = this.insert(key, value, key.hashCode(), false).invoke();

        return result.isRight() ? null : result.left();
    }

    /**
     * @param key
     * @param value
     * @return
     */
    @Override
    public V putIfAbsent(K key, V value) throws NullPointerException {
        Objects.requireNonNull(key);

        Either<V, Status> result = this.insert(key, value, key.hashCode(), true).invoke();

        return result.isRight() ? null : result.left();
    }

    /**
     * @param map
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> map) throws NullPointerException {
        Objects.requireNonNull(map);

        // TODO: make this atomic?
        map.forEach(this::put);
    }

    /*
     *
     *
     * @param key
     * @param value
     * @param hashCode
     * @param onlyIfAbsent
     * @return
     */
    @TailRecursive
    private TailCall<Either<V, Status>> insert(K key, V value, int hashCode, boolean onlyIfAbsent) {
        IndirectionNode<K, V> root = this.rdcssReadRoot();
        Either<V, Status> result = root.insert(key, value, hashCode, onlyIfAbsent, this);
        if (result.isRight() && result.right().equals(Status.RESTART)) {
            return TailCalls.call(() -> this.insert(key, value, hashCode, onlyIfAbsent));
        }

        return TailCalls.done(result.isRight() ? Either.left(null) : result);
    }

    /**
     * @param key
     * @return
     * @throws NullPointerException
     */
    @Override
    public V remove(Object key) throws NullPointerException {
        Objects.requireNonNull(key);

        @SuppressWarnings("unchecked")
        K castedKey = (K) key;

        // TODO: make it recursive, like insert().
        IndirectionNode<K, V> root = this.rdcssReadRoot();
        return root.remove(castedKey, null, castedKey.hashCode(), this);
    }

    /**
     * @return
     */
    @Override
    public int size() {
        return this.rdcssReadRoot().size();
    }

    /**
     *
     * @return
     */
    public long count() {
        return StreamSupport.stream(this.spliterator(), false).count();
    }

    /**
     * @return
     */
    @Override
    public boolean isEmpty() {
        return this.rdcssReadRoot().size() != 0;
    }

    /**
     *
     */
    @Override
    public void clear() {
        this.recursiveClear().invoke();
    }

    /*
     *
     * @return
     */
    @TailRecursive
    private TailCall<Void> recursiveClear() {
        IndirectionNode<K, V> oldRoot = this.rdcssReadRoot();
        IndirectionNode<K, V> newRoot = new IndirectionNode<>();

        if (!this.rdcssRoot(oldRoot, oldRoot.genCaSRead(this), newRoot)) {
            return TailCalls.call(this::recursiveClear);
        }
        return TailCalls.done(null);
    }

    /**
     * @return
     */
    @Override
    public Set<K> keySet() {
        return this.stream()
                .map(Node::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * @return
     */
    @Override
    public Collection<V> values() {
        return this.stream()
                .map(Node::getValue)
                .collect(Collectors.toList());
    }

    /**
     * @return
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return this.stream()
                .map(p -> new AbstractMap.SimpleEntry<>(p.getKey(), p.getValue()))
                .collect(Collectors.toSet());
    }

    /**
     * @return
     */
    public Stream<Node<K, V>> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    /**
     *
     * @param keyMapper
     * @param valueMapper
     * @param <T>
     * @param <K>
     * @param <V>
     * @return
     */
    public static <T, K, V> Collector<T, ?, ConcurrentTrieMap<K, V>> collector(Function<? super T, K> keyMapper, Function<? super T, V> valueMapper) {
        return Collector.of(
                ConcurrentTrieMap::new,
                (p, q) -> p.put(keyMapper.apply(q), valueMapper.apply(q)),
                (r, s) -> {
                    r.putAll(s);
                    return r;
                }
        );
    }

    /**
     * @return
     */
    public ConcurrentTrieMap<K, V> snapshot() {
        return this.recursiveSnapshot(false).invoke();
    }


    /**
     * @param readOnly
     * @return
     */
    public ConcurrentTrieMap<K, V> snapshot(boolean readOnly) {
        // TODO: doesn't work with readOnly == true, why?
        return this.recursiveSnapshot(readOnly).invoke();
    }

    /*
     *
     * @param readOnly
     * @return
     */
    @TailRecursive
    private TailCall<ConcurrentTrieMap<K, V>> recursiveSnapshot(boolean readOnly) {
        IndirectionNode<K, V> root = this.rdcssReadRoot();
        MainNode<K, V> rootMainNode = root.genCaSRead(this);

        if (this.rdcssRoot(root, rootMainNode, root.copyToGeneration(new Generation(), this))) {
            return TailCalls.done(
                    new ConcurrentTrieMap<>(
                            readOnly ? root : root.copyToGeneration(new Generation(), this),
                            readOnly));
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
            return new ConcurrentTrieIterator<>(this);
        } catch (IteratorException cti) {
            return ConcurrentTrieIterator.empty();
        }
    }

    /*
     * @return
     */
    Boolean isReadOnly() {
        return this.readOnly.get();
    }

    // RDCSS methods.
    // From Harris, Fraser, Pratt A practical multi-word compare-and-swap operation.
    // https://timharris.uk/papers/2002-disc.pdf

    /*
     * @return
     */
    IndirectionNode<K, V> rdcssReadRoot() {
        return this.rdcssReadRoot(false);
    }

    /*
     * @param abort
     * @return
     */
    IndirectionNode<K, V> rdcssReadRoot(boolean abort) {
        Object potentialRoot = ConcurrentTrieMap.ROOT_NODE_UPDATER.get(this);
        if (potentialRoot instanceof IndirectionNode) {
            @SuppressWarnings("unchecked")
            IndirectionNode<K, V> root = (IndirectionNode<K, V>) potentialRoot;
            return root;
        }

        // In any other case, it has to be a RDCSSDescriptor, then.
        return this.rdcssComplete(abort).invoke();
    }

    /*
     * @param oldNode
     * @param expectedNode
     * @param newNode
     * @return
     */
    private boolean rdcssRoot(IndirectionNode<K, V> oldNode, MainNode<K, V> expectedNode, IndirectionNode<K, V> newNode) {
        RDCSSDescriptor<K, V> descriptor = new RDCSSDescriptor<>(oldNode, newNode, expectedNode);
        if (ConcurrentTrieMap.ROOT_NODE_UPDATER.compareAndSet(this, oldNode, descriptor)) {
            this.rdcssComplete(false).invoke();
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
    @TailRecursive
    private TailCall<IndirectionNode<K, V>> rdcssComplete(boolean abort) {
        Object potentialRoot = ConcurrentTrieMap.ROOT_NODE_UPDATER.get(this);

        if (potentialRoot instanceof IndirectionNode) {
            @SuppressWarnings("unchecked")
            IndirectionNode<K, V> root = (IndirectionNode<K, V>) potentialRoot;
            return TailCalls.done(root);
        } else if (potentialRoot instanceof RDCSSDescriptor) {
            @SuppressWarnings("unchecked")
            RDCSSDescriptor<K, V> descriptor = new RDCSSDescriptor<>((RDCSSDescriptor<K, V>) potentialRoot);

            if (abort) {
                if (ConcurrentTrieMap.ROOT_NODE_UPDATER.compareAndSet(this, descriptor, descriptor.oldNode)) {
                    return TailCalls.done(descriptor.oldNode);
                }
            } else {
                MainNode<K, V> oldMainNode = descriptor.oldNode.genCaSRead(this);
                if (oldMainNode.equals(descriptor.expectedNode)) {
                    if (ConcurrentTrieMap.ROOT_NODE_UPDATER.compareAndSet(this, potentialRoot, descriptor.newNode)) {
                        descriptor.committed.set(true);
                        return TailCalls.done(descriptor.newNode);
                    }
                } else {
                    if (ConcurrentTrieMap.ROOT_NODE_UPDATER.compareAndSet(this, potentialRoot, descriptor.oldNode)) {
                        return TailCalls.done(descriptor.oldNode);
                    }
                }
            }
        }
        return TailCalls.call(() -> this.rdcssComplete(abort));
    }

    private static class RDCSSDescriptor<K, V> {
        private AtomicBoolean committed;
        private IndirectionNode<K, V> oldNode;
        private IndirectionNode<K, V> newNode;
        private MainNode<K, V> expectedNode;

        RDCSSDescriptor(RDCSSDescriptor<K, V> other) {
            this.committed = other.committed;
            this.oldNode = other.oldNode;
            this.newNode = other.newNode;
            this.expectedNode = other.expectedNode;
        }

        RDCSSDescriptor(IndirectionNode<K, V> oldNode, IndirectionNode<K, V> newNode, MainNode<K, V> expectedNode) {
            this.committed = new AtomicBoolean(false);
            this.oldNode = oldNode;
            this.newNode = newNode;
            this.expectedNode = expectedNode;
        }

        boolean isCommitted() {
            return this.committed.get();
        }
    }
}
