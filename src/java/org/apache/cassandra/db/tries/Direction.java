/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

/**
 * Class used to specify the direction of iteration. Provides methods used to replace comparisons and values in typical
 * loops and allow code to be written without explicit direction checks.
 * <p>
 * For example, iterating between l and r inclusive in forward direction is usually done as<br/>
 * {@code for (int i = l; i <= r; ++i) ...}
 * <p>
 * To loop over them in the specified direction dir, the loop above would change to<br/>
 * {@code for (int i = dir.start(l, r); dir.le(i, dir.end(l, r)); i += dir.increase) ...}
 */
public enum Direction
{
    FORWARD(1)
    {
        public int start(int left, int right)
        {
            return left;
        }

        public int end(int left, int right)
        {
            return right;
        }

        public boolean lt(int left, int right)
        {
            return left < right;
        }

        public boolean le(int left, int right)
        {
            return left <= right;
        }

        public int min(int left, int right)
        {
            return Math.min(left, right);
        }

        public int max(int left, int right)
        {
            return Math.max(left, right);
        }

        public <T> T select(T forward, T reverse)
        {
            return forward;
        }

        public int select(int forward, int reverse)
        {
            return forward;
        }

        public boolean isForward()
        {
            return true;
        }

        public Direction opposite()
        {
            return REVERSE;
        }
    },
    REVERSE(-1)
    {
        public int start(int left, int right)
        {
            return right;
        }

        public int end(int left, int right)
        {
            return left;
        }

        public boolean lt(int left, int right)
        {
            return left > right;
        }

        public boolean le(int left, int right)
        {
            return left >= right;
        }

        public int min(int left, int right)
        {
            return Math.max(left, right);
        }

        public int max(int left, int right)
        {
            return Math.min(left, right);
        }

        public <T> T select(T forward, T reverse)
        {
            return reverse;
        }

        public int select(int forward, int reverse)
        {
            return reverse;
        }

        public boolean isForward()
        {
            return false;
        }

        public Direction opposite()
        {
            return FORWARD;
        }
    };

    /** Value that needs to be added to advance the iteration, i.e. value corresponding to 1 */
    public final int increase;

    Direction(int increase)
    {
        this.increase = increase;
    }

    /** Returns the value to start iteration with, i.e. the bound corresponding to l for the forward direction */
    public abstract int start(int l, int r);
    /** Returns the value to end iteration with, i.e. the bound corresponding to r for the forward direction */
    public abstract int end(int l, int r);
    /** Returns the result of the operation corresponding to a<b for the forward direction */
    public abstract boolean lt(int a, int b);
    /** Returns the result of the operation corresponding to a<=b for the forward direction */
    public abstract boolean le(int a, int b);
    /** Returns the result of the operation corresponding to min(a, b) for the forward direction */
    public abstract int min(int a, int b);
    /** Returns the result of the operation corresponding to max(a, b) for the forward direction */
    public abstract int max(int a, int b);

    /**
     * Use the first argument in forward direction and the second in reverse, i.e. isForward() ? forward : reverse.
     */
    public abstract <T> T select(T forward, T reverse);

    /**
     * Use the first argument in forward direction and the second in reverse, i.e. isForward() ? forward : reverse.
     */
    public abstract int select(int forward, int reverse);

    public abstract boolean isForward();

    public abstract Direction opposite();
}
