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

package org.opendc.compute.core.metal.service

import kotlinx.coroutines.CancellationException
import org.opendc.compute.core.image.Image
import org.opendc.compute.core.metal.Node
import org.opendc.compute.core.metal.driver.BareMetalDriver

/**
 * A very basic implementation of the [ProvisioningService].
 */
public class SimpleProvisioningService : ProvisioningService {
    /**
     * The active nodes in this service.
     */
    private val nodes: MutableMap<Node, BareMetalDriver> = mutableMapOf()

    override suspend fun create(driver: BareMetalDriver): Node {
        val node = driver.init()
        nodes[node] = driver
        return node
    }

    override suspend fun nodes(): Set<Node> = nodes.keys

    override suspend fun refresh(node: Node): Node {
        return nodes[node]!!.refresh()
    }

    override suspend fun deploy(node: Node, image: Image): Node {
        val driver = nodes[node]!!
        driver.setImage(image)
        return driver.reboot()
    }

    override suspend fun stop(node: Node): Node {
        val driver = nodes[node]!!
        return try {
            driver.stop()
        } catch (e: CancellationException) {
            node
        }
    }

    public fun allNodes() : Map<Node, BareMetalDriver> {
        return nodes.toMap()
    }
}
