/*
 * Copyright (c) 2020 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.compute.simulator.allocation

import org.opendc.compute.simulator.HypervisorView

/**
 * An [AllocationPolicy] that selects the machine with the highest/lowest amount of memory per core.
 *
 * @param reversed An option to reverse the order of the machines (lower amount of memory scores better).
 */
public class AvailableCoreMemoryAllocationPolicy(private val reversed: Boolean = false) : AllocationPolicy {
    override fun invoke(): AllocationPolicy.Logic = object : ComparableAllocationPolicyLogic {
        override val comparator: Comparator<HypervisorView> =
            compareBy<HypervisorView> { -it.availableMemory / it.server.flavor.cpuCount }
                .run { if (reversed) reversed() else this }
    }
}
