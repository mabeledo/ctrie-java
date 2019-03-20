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

public interface Either<T, U> {
    static <T, U> Either<T, U> left(T left) { return new Left<>(left); }

    static <T, U> Either<T, U> right(U right) {
        return new Right<>(right);
    }

    default boolean isLeft() { return false; }
    default T left() { return null; }

    default boolean isRight() { return false; }
    default U right() { return null; }

    final class Left<T, U> implements Either<T, U> {
        private final T value;

        private Left(T value) {
            this.value = value;
        }

        @Override
        public boolean isLeft() {
            return true;
        }

        @Override
        public T left() {
            return this.value;
        }
    }

    final class Right<T, U> implements Either<T, U> {
        private final U value;

        private Right(@NotNull U value) {
            this.value = value;
        }

        @Override
        public boolean isRight() {
            return true;
        }

        @Override
        public U right() {
            return this.value;
        }
    }
}