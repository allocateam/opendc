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
import org.opendc.compute.simulator.SimVirtProvisioningService

/**
 * The logic for an [AllocationPolicy] that uses a [Comparator] to select the appropriate node.
 */
public interface ComparableAllocationPolicyLogic : AllocationPolicy.Logic {
    /**
     * The comparator to use.
     */
    public val comparator: Comparator<HypervisorView>

    override fun select(
        hypervisors: Set<HypervisorView>,
        image: SimVirtProvisioningService.ImageView
    ): HypervisorView? {
        // Choose hypervisor that has enough memory and CPU available with the lowest uid
        // or return null
        return hypervisors.asSequence()
            .filter { hv ->
                val fitsMemory = hv.availableMemory >= (image.image.tags["required-memory"] as Long)
                val fitsCpu = hv.server.flavor.cpuCount >= image.flavor.cpuCount
                fitsMemory && fitsCpu
            }
            .minWithOrNull(comparator.thenBy { it.server.uid })
    }
}
