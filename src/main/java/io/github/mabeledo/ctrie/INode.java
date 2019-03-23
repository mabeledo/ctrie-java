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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

class INode<K, V> implements Node<K, V> {
    private static final AtomicReferenceFieldUpdater<INode, MainNode> MAIN_NODE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(INode.class, MainNode.class, "mainNode");
    private volatile MainNode<K, V> mainNode;

    private Generation generation;

    INode() {
        this.generation = new Generation();
        this.mainNode = new CNode<>(this.generation);
    }

    INode(Generation generation) {
        this.generation = generation;
        this.mainNode = new CNode<>(this.generation);
    }

    INode(MainNode<K, V> mainNode, Generation generation) {
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
     * @param cTrie
     * @return
     */
    @NotNull
    Either<V, Status> lookup(@NotNull K key, int hashCode, int level, INode<K, V> parent, @NotNull Generation startGeneration, CTrie<K, V> cTrie) {
        MainNode<K, V> mainNode = this.genCaSRead(cTrie);

        if (mainNode instanceof CNode) {
            CNode<K, V> cNode = (CNode<K, V>) mainNode;
            FlagPos flagPos = new FlagPos(hashCode, level, cNode.getBitmap());

            if ((cNode.getBitmap() & flagPos.getFlag()) == 0) {
                return Either.right(Status.NOT_FOUND);
            }

            Node<K, V> node = cNode.getChild(flagPos.getPos());
            if (node instanceof INode) {
                INode<K, V> iNode = (INode<K, V>) node;
                if (cTrie.isReadOnly() || (Objects.equals(startGeneration, iNode.generation))) {
                    // Not found yet, this is an INode, but this is an actual branch, so let's keep moving.
                    return iNode.lookup(key, hashCode, level + 5, this, startGeneration, cTrie);
                } else {
                    if (this.genCaS(cNode, cNode.renew(startGeneration, cTrie), cTrie)) {
                        // Try again!
                        return this.lookup(key, hashCode, level, parent, startGeneration, cTrie);
                    } else {
                        return Either.right(Status.RESTART);
                    }
                }
            } else {
                // Leaf.
                SingletonNode<K, V> singletonNode = (SingletonNode<K, V>) node;
                return Objects.equals(singletonNode.getKey(), key) ?
                        Either.left(singletonNode.getValue()) :
                        Either.right(Status.NOT_FOUND);
            }

        } else if (mainNode instanceof TombNode) {
            // Tomb node.
            if (cTrie.isReadOnly()) {
                // Look for a potential value.
                TombNode<K, V> tombNode = (TombNode<K, V>) mainNode;
                if (Objects.equals(tombNode.getKey(), key) && (tombNode.getHashCode() == hashCode)) {
                    return Either.left(tombNode.getValue());
                } else {
                    return Either.right(Status.NOT_FOUND);
                }
            } else {
                // Clean and try again.
                this.clean(parent, cTrie, level - 5);
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
     * @param cTrie
     * @return
     */
    @NotNull
    @TailRecursive
    TailCall<Either<V, Status>> insert(@NotNull K key, V value, int hashCode, int level, INode<K, V> parent, @NotNull Generation startGeneration, CTrie<K, V> cTrie) {
        MainNode<K, V> mainNode = this.genCaSRead(cTrie);

        if (mainNode instanceof CNode) {
            CNode<K, V> cNode = (CNode<K, V>) mainNode;
            int index = (hashCode >>> level) & 0x1f;
            int flag = 1 << index;
            int bitmap = cNode.getBitmap();
            int mask = flag - 1;
            int pos = Integer.bitCount(bitmap & mask);

            if ((bitmap & flag) != 0) {
                Node<K, V> node = cNode.getChild(pos);
                if (node instanceof INode) {
                    INode<K, V> iNode = (INode<K, V>) node;
                    if (Objects.equals(startGeneration, iNode.getGeneration())) {
                        return TailCalls.call(() -> iNode.insert(key, value, hashCode, level + 5, this, startGeneration, cTrie));
                    } else {
                        if (this.genCaS(cNode, cNode.renew(startGeneration, cTrie), cTrie)) {
                            return TailCalls.call(() -> this.insert(key, value, hashCode, level, parent, startGeneration, cTrie));
                        }

                        return TailCalls.done(Either.right(Status.RESTART));
                    }
                } else if (node instanceof SingletonNode) {
                    SingletonNode<K, V> singletonNode = (SingletonNode<K, V>) node;
                    if (Objects.equals(singletonNode.getKey(), key) && (singletonNode.getHashCode() == hashCode)) {
                        if (this.genCaS(cNode, cNode.updateAt(pos, new SingletonNode<>(key, value, hashCode), this.generation), cTrie)) {
                            return TailCalls.done(Either.left(singletonNode.getValue()));
                        }

                        return TailCalls.done(Either.right(Status.RESTART));
                    } else {
                        // Key didn't exist, new value will be inserted.
                        CNode<K, V> renewedNode = Objects.equals(cNode.getGeneration(), this.generation) ? cNode : cNode.renew(this.generation, cTrie);
                        CNode<K, V> updatedRenewedNode =
                                renewedNode.updateAt(
                                        pos,
                                        new INode<>(MainNode.dual(singletonNode, singletonNode.getHashCode(), new SingletonNode<>(key, value, hashCode), hashCode, level + 5, this.generation), this.generation),
                                        this.generation);
                        if (this.genCaS(cNode, updatedRenewedNode, cTrie)) {
                            return TailCalls.done(Either.left(null));
                        }

                        return TailCalls.done(Either.right(Status.RESTART));
                    }
                }
            } else {
                CNode<K, V> renewedNode = Objects.equals(cNode.getGeneration(), this.generation) ? cNode : cNode.renew(this.generation, cTrie);

                if (this.genCaS(cNode, renewedNode.insertAt(pos, flag, new SingletonNode<>(key, value, hashCode), this.generation), cTrie)) {
                    return TailCalls.done(Either.left(null));
                }

                return TailCalls.done(Either.right(Status.RESTART));
            }

        } else if (mainNode instanceof TombNode) {
            //
            this.clean(parent, cTrie, level - 5);
            return TailCalls.done(Either.right(Status.RESTART));
        } else if (mainNode instanceof LeafNode) {
            //
            LeafNode<K, V> leafNode = (LeafNode<K, V>) mainNode;
            LeafNode<K, V> updatedLeafNode = leafNode.insert(key, value);

            if (this.genCaS(leafNode, updatedLeafNode, cTrie)) {
                return TailCalls.done(leafNode.get(key));
            }

            return TailCalls.done(Either.right(Status.RESTART));
        }

        // Try again by default, although it should never reach this point.
        return TailCalls.done(Either.right(Status.RESTART));
    }

    /**
     * Get the current main node of this INode.
     *
     * @param cTrie the current CTrie structure where this INode lives.
     * @return the current main node of this INode.
     */
    MainNode<K, V> genCaSRead(@NotNull CTrie<K, V> cTrie) {
        @SuppressWarnings("unchecked")
        MainNode<K, V> mainNode = (MainNode<K, V>) INode.MAIN_NODE_UPDATER.get(this);
        MainNode<K, V> previousMainNode = mainNode.readPrevious();

        if (Objects.isNull(previousMainNode)) {
            return mainNode;
        }

        return this.genCaSCommit(mainNode, cTrie);
    }

    /*
     * Finally store a change proposed in the tree.
     * This method will check if the new node can be stored in the tree, and where.
     * Once it finds a place, it's committed.
     *
     * @param node the node being committed.
     * @param cTrie the current CTrie structure where this INode lives.
     * @return the committed node.
     */
    private MainNode<K, V> genCaSCommit(MainNode<K, V> node, @NotNull CTrie<K, V> cTrie) {
        MainNode<K, V> previousNode = node.readPrevious();
        INode<K, V> cTrieRoot = cTrie.rdcssReadRoot(true);

        if (Objects.isNull(previousNode)) {
            return node;
        } else if (previousNode instanceof FailedNode) {
            if (INode.MAIN_NODE_UPDATER.compareAndSet(this, node, previousNode.readPrevious())) {
                return previousNode.readPrevious();
            } else {
                @SuppressWarnings("unchecked")
                MainNode<K, V> mainNode = (MainNode<K, V>) INode.MAIN_NODE_UPDATER.get(this);
                return this.genCaSCommit(mainNode, cTrie);
            }
        } else {
            if (Objects.equals(cTrieRoot.generation, this.generation) && !cTrie.isReadOnly()) {
                return
                        node.casPrevious(previousNode, null) ?
                                node :
                                this.genCaSCommit(node, cTrie);
            } else {
                node.casPrevious(previousNode, new FailedNode<>(previousNode));

                @SuppressWarnings("unchecked")
                MainNode<K, V> mainNode = (MainNode<K, V>) INode.MAIN_NODE_UPDATER.get(this);
                return this.genCaSCommit(mainNode, cTrie);
            }
        }
    }

    /*
     * Generational compare and set.
     * Set the value of the current INode main node.
     *
     * @param oldNode the previous value of this INode main node.
     * @param newNode the proposed new value of this INode main node.
     * @param cTrie the current CTrie structure where this INode lives.
     * @return true if the operation succeeds, false otherwise.
     */
    private boolean genCaS(MainNode<K, V> oldNode, MainNode<K, V> newNode, CTrie<K, V> cTrie) {
        newNode.writePrevious(oldNode);

        if (INode.MAIN_NODE_UPDATER.compareAndSet(this, oldNode, newNode)) {
            this.genCaSCommit(newNode, cTrie);
            return Objects.isNull(newNode.readPrevious());
        }
        return false;
    }

    /**
     * Copy the current INode to a new generation.
     *
     * @param generation the generation this INode is being copied to.
     * @param cTrie      the current CTrie.
     * @return the newly copied INode.
     */
    INode<K, V> copyToGeneration(Generation generation, CTrie<K, V> cTrie) {
        INode<K, V> newINode = new INode<>(generation);
        MainNode<K, V> mainNode = this.genCaSRead(cTrie);

        INode.MAIN_NODE_UPDATER.set(newINode, mainNode);

        return newINode;
    }

    /*
     *
     * @param level
     */
    private void clean(INode<K, V> parent, CTrie<K, V> cTrie, int level) {
        MainNode<K, V> mainNode = parent.genCaSRead(cTrie);
        if (mainNode instanceof CNode) {
            CNode<K, V> cNode = (CNode<K, V>) mainNode;
            parent.genCaS(cNode, cNode.compress(cTrie, level, this.generation), cTrie);
        }
    }
}