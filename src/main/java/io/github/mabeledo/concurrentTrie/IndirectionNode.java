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

import javax.validation.constraints.NotNull;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

class IndirectionNode<K, V> implements Node<K, V> {
    private static final AtomicReferenceFieldUpdater<IndirectionNode, MainNode> MAIN_NODE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(IndirectionNode.class, MainNode.class, "mainNode");
    private volatile MainNode<K, V> mainNode;

    private Generation generation;

    IndirectionNode() {
        this.generation = new Generation();
        this.mainNode = new CNode<>(this.generation);
    }

    IndirectionNode(Generation generation) {
        this.generation = generation;
        this.mainNode = new CNode<>(this.generation);
    }

    IndirectionNode(MainNode<K, V> mainNode, Generation generation) {
        this.mainNode = mainNode;
        this.generation = generation;
    }


    /**
     * @return
     */
    Generation getGeneration() {
        return this.generation;
    }

    /**
     * @param key
     * @param hashCode
     * @param level
     * @param parent
     * @param startGeneration
     * @param concurrentTrieMap
     * @return
     */
    @NotNull
    Either<V, Status> lookup(K key, int hashCode, int level, IndirectionNode<K, V> parent, @NotNull Generation startGeneration, ConcurrentTrieMap<K, V> concurrentTrieMap) {
        MainNode<K, V> mainNode = this.genCaSRead(concurrentTrieMap);

        if (mainNode instanceof CNode) {
            CNode<K, V> cNode = (CNode<K, V>) mainNode;
            int index = (hashCode >>> level) & 0x1f;
            int flag = 1 << index;
            int bitmap = cNode.getBitmap();

            if ((bitmap & flag) == 0) {
                return Either.right(Status.NOT_FOUND);
            }

            int pos = (bitmap == 0xffffffff) ? index : Integer.bitCount(bitmap & (flag - 1));

            Node<K, V> node = cNode.getChild(pos);
            if (node instanceof IndirectionNode) {
                IndirectionNode<K, V> indirectionNode = (IndirectionNode<K, V>) node;
                if (concurrentTrieMap.isReadOnly() || (Objects.equals(startGeneration, indirectionNode.generation))) {
                    // Not found yet, this is an IndirectionNode, but this is an actual branch, so let's keep moving.
                    return indirectionNode.lookup(key, hashCode, level + 5, this, startGeneration, concurrentTrieMap);
                } else {
                    if (this.genCaS(cNode, cNode.renew(startGeneration, concurrentTrieMap), concurrentTrieMap)) {
                        // Try again!
                        return this.lookup(key, hashCode, level, parent, startGeneration, concurrentTrieMap);
                    } else {
                        return Either.right(Status.RESTART);
                    }
                }
            } else {
                SingletonNode<K, V> singletonNode = (SingletonNode<K, V>) node;
                return Objects.equals(singletonNode.getKey(), key) && (singletonNode.getHashCode() == hashCode) ?
                        Either.left(singletonNode.getValue()) :
                        Either.right(Status.NOT_FOUND);
            }

        } else if (mainNode instanceof TombNode) {
            // Tomb node.
            if (concurrentTrieMap.isReadOnly()) {
                // Look for a potential value.
                TombNode<K, V> tombNode = (TombNode<K, V>) mainNode;
                if (Objects.equals(tombNode.getKey(), key) && (tombNode.getHashCode() == hashCode)) {
                    return Either.left(tombNode.getValue());
                } else {
                    return Either.right(Status.NOT_FOUND);
                }
            } else {
                // Clean and try again.
                this.clean(parent, concurrentTrieMap, level - 5);
                return Either.right(Status.RESTART);
            }
        } else if (mainNode instanceof LeafNode) {
            return ((LeafNode<K, V>) mainNode).get(key);
        }

        return Either.right(Status.NOT_FOUND);
    }

    /**
     * @param key
     * @param value
     * @param hashCode
     * @param level
     * @param parent
     * @param startGeneration
     * @param onlyIfAbsent
     * @param concurrentTrieMap
     * @return
     */
    @NotNull
    @TailRecursive
    TailCall<Either<V, Status>> insert(
            @NotNull K key,
            V value,
            int hashCode,
            int level,
            IndirectionNode<K, V> parent,
            @NotNull Generation startGeneration,
            boolean onlyIfAbsent,
            ConcurrentTrieMap<K, V> concurrentTrieMap) {

        MainNode<K, V> mainNode = this.genCaSRead(concurrentTrieMap);

        if (mainNode instanceof CNode) {
            CNode<K, V> cNode = (CNode<K, V>) mainNode;
            int index = (hashCode >>> level) & 0x1f;
            int flag = 1 << index;
            int bitmap = cNode.getBitmap();
            int mask = flag - 1;
            int pos = Integer.bitCount(bitmap & mask);

            if ((bitmap & flag) != 0) {
                Node<K, V> node = cNode.getChild(pos);
                if (node instanceof IndirectionNode) {
                    IndirectionNode<K, V> indirectionNode = (IndirectionNode<K, V>) node;
                    if (Objects.equals(startGeneration, indirectionNode.getGeneration())) {
                        return TailCalls.call(() -> indirectionNode.insert(key, value, hashCode, level + 5, this, startGeneration, onlyIfAbsent, concurrentTrieMap));
                    } else {
                        if (this.genCaS(cNode, cNode.renew(startGeneration, concurrentTrieMap), concurrentTrieMap)) {
                            return TailCalls.call(() -> this.insert(key, value, hashCode, level, parent, startGeneration, onlyIfAbsent, concurrentTrieMap));
                        }

                        return TailCalls.done(Either.right(Status.RESTART));
                    }
                } else if (node instanceof SingletonNode) {
                    SingletonNode<K, V> singletonNode = (SingletonNode<K, V>) node;
                    if (Objects.equals(singletonNode.getKey(), key) && (singletonNode.getHashCode() == hashCode)) {
                        if (this.genCaS(cNode, cNode.updateAt(pos, new SingletonNode<>(key, value, hashCode), this.generation), concurrentTrieMap)) {
                            return TailCalls.done(Either.left(singletonNode.getValue()));
                        }

                        return TailCalls.done(Either.right(Status.RESTART));
                    } else {
                        // Key didn't exist, new value will be inserted.
                        CNode<K, V> renewedNode =
                                Objects.equals(cNode.getGeneration(), this.generation) ?
                                        cNode :
                                        cNode.renew(this.generation, concurrentTrieMap);
                        CNode<K, V> updatedRenewedNode =
                                renewedNode.updateAt(
                                        pos,
                                        new IndirectionNode<>(
                                                MainNode.dual(
                                                        singletonNode,
                                                        singletonNode.getHashCode(),
                                                        new SingletonNode<>(key, value, hashCode),
                                                        hashCode,
                                                        level + 5,
                                                        this.generation),
                                                this.generation),
                                        this.generation);
                        if (this.genCaS(cNode, updatedRenewedNode, concurrentTrieMap)) {
                            return TailCalls.done(Either.left(null));
                        }

                        return TailCalls.done(Either.right(Status.RESTART));
                    }
                }
            } else {
                CNode<K, V> renewedNode =
                        Objects.equals(cNode.getGeneration(), this.generation) ?
                                cNode :
                                cNode.renew(this.generation, concurrentTrieMap);

                if (this.genCaS(
                        cNode,
                        renewedNode.insertAt(pos, flag, new SingletonNode<>(key, value, hashCode), this.generation),
                        concurrentTrieMap)) {
                    return TailCalls.done(Either.left(null));
                }

                return TailCalls.done(Either.right(Status.RESTART));
            }

        } else if (mainNode instanceof TombNode) {
            //
            this.clean(parent, concurrentTrieMap, level - 5);
            return TailCalls.done(Either.right(Status.RESTART));
        } else if (mainNode instanceof LeafNode) {
            //
            LeafNode<K, V> leafNode = (LeafNode<K, V>) mainNode;
            LeafNode<K, V> updatedLeafNode = leafNode.insert(key, value, onlyIfAbsent);

            if (this.genCaS(leafNode, updatedLeafNode, concurrentTrieMap)) {
                Either<V, Status> previousValue = leafNode.get(key);
                return TailCalls.done(previousValue.isLeft() ? previousValue : Either.left(null));
            }

            return TailCalls.done(Either.right(Status.RESTART));
        }

        // Try again by default, although it should never reach this point.
        return TailCalls.done(Either.right(Status.RESTART));
    }

    /**
     * Not quite tail recursive at all, btw, but lazy evaluation always helps a little.
     *
     * @param key
     * @param value             a value <V>. If not null only pairs containing this key and value. If null, any value linked to the key will be removed.
     * @param hashCode
     * @param level
     * @param parent
     * @param startGeneration
     * @param concurrentTrieMap
     * @return an Either containing the status of the operation, or the removed value.
     */
    @NotNull
    Either<V, Status> remove(@NotNull K key, V value, int hashCode, int level, IndirectionNode<K, V> parent, @NotNull Generation startGeneration, ConcurrentTrieMap<K, V> concurrentTrieMap) {
        MainNode<K, V> mainNode = this.genCaSRead(concurrentTrieMap);

        if (mainNode instanceof CNode) {
            CNode<K, V> cNode = (CNode<K, V>) mainNode;

            int index = (hashCode >>> level) & 0x1f;
            int flag = 1 << index;
            int bitmap = cNode.getBitmap();

            if ((bitmap & flag) == 0) {
                return Either.right(Status.NOT_FOUND);
            }

            int pos = Integer.bitCount(bitmap & (flag - 1));
            Node<K, V> childNode = cNode.getChild(pos);
            Either<V, Status> result;

            if (childNode instanceof IndirectionNode) {
                IndirectionNode<K, V> indirectionNode = (IndirectionNode<K, V>) childNode;

                if (Objects.equals(startGeneration, indirectionNode.getGeneration())) {
                    result = indirectionNode.remove(key, value, hashCode, level + 5, this, startGeneration, concurrentTrieMap);
                } else {
                    if (this.genCaS(cNode, cNode.renew(startGeneration, concurrentTrieMap), concurrentTrieMap)) {
                        result = this.remove(key, value, hashCode, level, parent, startGeneration, concurrentTrieMap);
                    } else {
                        result = Either.right(Status.RESTART);
                    }
                }

            } else if (childNode instanceof SingletonNode) {
                SingletonNode<K, V> singletonNode = (SingletonNode<K, V>) childNode;

                if (Objects.equals(singletonNode.getKey(), key) && (singletonNode.getHashCode() == hashCode) &&
                        ((value == null) || (Objects.equals(value, singletonNode.getValue())))) {
                    MainNode<K, V> updatedNode =
                            cNode.removeAt(pos, flag, this.generation).contract(level);
                    if (this.genCaS(cNode, updatedNode, concurrentTrieMap)) {
                        result = Either.left(singletonNode.getValue());
                    } else {
                        result = Either.right(Status.RESTART);
                    }
                } else {
                    result = Either.right(Status.NOT_FOUND);
                }
            } else {
                result = Either.right(Status.RESTART);
            }

            if (result.isRight()) {
                return result;
            }

            if (Objects.nonNull(parent)) {
                Node<K, V> node = this.genCaSRead(concurrentTrieMap);
                if (node instanceof TombNode) {
                    this.cleanParent(hashCode, level, node, parent, startGeneration, concurrentTrieMap).invoke();
                }
            }

            return result;

        } else if (mainNode instanceof TombNode) {
            this.clean(parent, concurrentTrieMap, level - 5);
            return Either.right(Status.RESTART);
        } else if (mainNode instanceof LeafNode) {
            LeafNode<K, V> leafNode = (LeafNode<K, V>) mainNode;

            if (Objects.isNull(value)) {
                Either<V, Status> potentialLeafNodeValue = leafNode.get(key);

                if (potentialLeafNodeValue.isLeft()) {
                    // Some value found.
                    MainNode<K, V> updatedNode = leafNode.remove(key);

                    if (this.genCaS(leafNode, updatedNode, concurrentTrieMap)) {
                        return potentialLeafNodeValue;
                    }

                    return Either.right(Status.RESTART);
                }

                // No value found, return Status.NOT_FOUND.
                return Either.right(Status.NOT_FOUND);
            } else {
                // Remove by key *and* value.
                Either<V, Status> potentialLeafNodeValue = leafNode.get(key);

                if (potentialLeafNodeValue.isLeft()) {
                    V storedValue = potentialLeafNodeValue.left();
                    if (Objects.equals(value, storedValue)) {
                        MainNode<K, V> updatedNode = leafNode.remove(key);

                        if (!this.genCaS(leafNode, updatedNode, concurrentTrieMap)) {
                            return Either.right(Status.RESTART);
                        }
                    }
                }

                return potentialLeafNodeValue;
            }
        }

        // Try again by default, although it should never reach this point.
        return Either.right(Status.RESTART);
    }

    /**
     * Get the current main node of this IndirectionNode.
     *
     * @param concurrentTrieMap the current ConcurrentTrieMap structure where this IndirectionNode lives.
     * @return the current main node of this IndirectionNode.
     */
    MainNode<K, V> genCaSRead(@NotNull ConcurrentTrieMap<K, V> concurrentTrieMap) {
        @SuppressWarnings("unchecked")
        MainNode<K, V> mainNode = (MainNode<K, V>) IndirectionNode.MAIN_NODE_UPDATER.get(this);
        MainNode<K, V> previousMainNode = mainNode.readPrevious();

        if (Objects.isNull(previousMainNode)) {
            return mainNode;
        }

        return this.genCaSCommit(mainNode, concurrentTrieMap);
    }

    /*
     * Finally store a change proposed in the tree.
     * This method will check if the new node can be stored in the tree, and where.
     * Once it finds a place, it's committed.
     *
     * @param node the node being committed.
     * @param concurrentTrieMap the current ConcurrentTrieMap structure where this IndirectionNode lives.
     * @return the committed node.
     */
    private MainNode<K, V> genCaSCommit(MainNode<K, V> node, @NotNull ConcurrentTrieMap<K, V> concurrentTrieMap) {
        if (Objects.isNull(node)) {
            return null;
        }

        MainNode<K, V> previousNode = node.readPrevious();
        IndirectionNode<K, V> cTrieRoot = concurrentTrieMap.rdcssReadRoot(true);

        if (Objects.isNull(previousNode)) {
            return node;
        } else if (previousNode instanceof FailedNode) {
            FailedNode<K, V> failedNode = (FailedNode<K, V>) previousNode;
            if (IndirectionNode.MAIN_NODE_UPDATER.compareAndSet(this, node, failedNode.readPrevious())) {
                return failedNode.readPrevious();
            } else {
                @SuppressWarnings("unchecked")
                MainNode<K, V> mainNode = (MainNode<K, V>) IndirectionNode.MAIN_NODE_UPDATER.get(this);
                return this.genCaSCommit(mainNode, concurrentTrieMap);
            }
        } else {
            if (Objects.equals(cTrieRoot.generation, this.generation) && !concurrentTrieMap.isReadOnly()) {
                return
                        node.casPrevious(previousNode, null) ?
                                node :
                                this.genCaSCommit(node, concurrentTrieMap);
            } else {
                node.casPrevious(previousNode, new FailedNode<>(previousNode));

                @SuppressWarnings("unchecked")
                MainNode<K, V> mainNode = (MainNode<K, V>) IndirectionNode.MAIN_NODE_UPDATER.get(this);
                return this.genCaSCommit(mainNode, concurrentTrieMap);
            }
        }
    }

    /*
     * Generational compare and set.
     * Set the value of the current IndirectionNode main node.
     *
     * @param oldNode the previous value of this IndirectionNode main node.
     * @param newNode the proposed new value of this IndirectionNode main node.
     * @param concurrentTrieMap the current ConcurrentTrieMap structure where this IndirectionNode lives.
     * @return true if the operation succeeds, false otherwise.
     */
    private boolean genCaS(MainNode<K, V> oldNode, MainNode<K, V> newNode, ConcurrentTrieMap<K, V> concurrentTrieMap) {
        newNode.writePrevious(oldNode);

        if (IndirectionNode.MAIN_NODE_UPDATER.compareAndSet(this, oldNode, newNode)) {
            this.genCaSCommit(newNode, concurrentTrieMap);
            return Objects.isNull(newNode.readPrevious());
        }
        return false;
    }

    /**
     * Copy the current IndirectionNode to a new generation.
     *
     * @param generation        the generation this IndirectionNode is being copied to.
     * @param concurrentTrieMap the current ConcurrentTrieMap.
     * @return the newly copied IndirectionNode.
     */
    IndirectionNode<K, V> copyToGeneration(Generation generation, ConcurrentTrieMap<K, V> concurrentTrieMap) {
        IndirectionNode<K, V> newIndirectionNode = new IndirectionNode<>(generation);
        MainNode<K, V> mainNode = this.genCaSRead(concurrentTrieMap);

        IndirectionNode.MAIN_NODE_UPDATER.set(newIndirectionNode, mainNode);

        return newIndirectionNode;
    }


    /*
     *
     * @param parent
     * @param concurrentTrieMap
     * @param level
     */
    private void clean(IndirectionNode<K, V> parent, ConcurrentTrieMap<K, V> concurrentTrieMap, int level) {
        MainNode<K, V> mainNode = parent.genCaSRead(concurrentTrieMap);
        if (mainNode instanceof CNode) {
            CNode<K, V> cNode = (CNode<K, V>) mainNode;
            parent.genCaS(cNode, cNode.compress(concurrentTrieMap, level, this.generation), concurrentTrieMap);
        }
    }

    /*
     *
     * @param hashCode
     * @param level
     * @param nonLiveNode
     * @param parent
     * @param startGeneration
     * @param concurrentTrieMap
     */
    @TailRecursive
    private TailCall<Void> cleanParent(int hashCode, int level, Object nonLiveNode, IndirectionNode<K, V> parent, @NotNull Generation startGeneration, ConcurrentTrieMap<K, V> concurrentTrieMap) {
        MainNode<K, V> parentMainNode = parent.genCaSRead(concurrentTrieMap);

        if (parentMainNode instanceof CNode) {
            CNode<K, V> cNode = (CNode<K, V>) parentMainNode;

            int index = (hashCode >>> (level - 5)) & 0x1f;
            int bitmap = cNode.getBitmap();
            int flag = 1 << index;

            if ((bitmap & flag) == 0) {
                return TailCalls.done(null);
            }

            int pos = Integer.bitCount(bitmap & (flag - 1));
            Node<K, V> node = cNode.getChild(pos);

            if (node.equals(this)) {
                if (nonLiveNode instanceof TombNode) {
                    @SuppressWarnings("unchecked")
                    TombNode<K, V> tombNode = (TombNode<K, V>) nonLiveNode;
                    MainNode<K, V> updatedCNode =
                            cNode
                                    .updateAt(pos, new SingletonNode<>(tombNode), this.generation)
                                    .contract(level - 5);

                    if (!parent.genCaS(cNode, updatedCNode, concurrentTrieMap)) {
                        if (Objects.equals(concurrentTrieMap.rdcssReadRoot().getGeneration(), startGeneration)) {
                            TailCalls.call(() ->
                                    this.cleanParent(hashCode, level, nonLiveNode, parent, startGeneration, concurrentTrieMap));
                        }
                    }
                }
            }
        }
        return TailCalls.done(null);
    }
}