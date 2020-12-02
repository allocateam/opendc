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

package org.opendc.experiments.allocateam.experiment

import mu.KotlinLogging
import org.opendc.compute.core.Server
import org.opendc.compute.core.ServerState
import org.opendc.compute.core.virt.driver.VirtDriver
import org.opendc.compute.core.virt.service.VirtProvisioningEvent
import org.opendc.experiments.sc20.experiment.monitor.ExperimentMonitor
import org.opendc.experiments.sc20.telemetry.HostEvent
import kotlin.math.max

/**
 * An [ExperimentMonitor] that tracks the aggregate metrics for each repeat.
 */
public class WebExperimentMonitor : ExperimentMonitor {
    private val logger = KotlinLogging.logger {}
    private val currentHostEvent = mutableMapOf<Server, HostEvent>()
    private var startTime = -1L

    override fun reportVmStateChange(time: Long, server: Server) {
        if (startTime < 0) {
            startTime = time

            // Update timestamp of initial event
            currentHostEvent.replaceAll { _, v -> v.copy(timestamp = startTime) }
        }
    }

    override fun reportHostStateChange(
        time: Long,
        driver: VirtDriver,
        server: Server
    ) {
        logger.debug { "Host ${server.uid} changed state ${server.state} [$time]" }

        val previousEvent = currentHostEvent[server]

        val roundedTime = previousEvent?.let {
            val duration = time - it.timestamp
            val k = 5 * 60 * 1000L // 5 min in ms
            val rem = duration % k

            if (rem == 0L) {
                time
            } else {
                it.timestamp + duration + k - rem
            }
        } ?: time

        reportHostSlice(
            roundedTime,
            0,
            0,
            0,
            0,
            0.0,
            0.0,
            0,
            server
        )
    }

    private val lastPowerConsumption = mutableMapOf<Server, Double>()

    override fun reportPowerConsumption(host: Server, draw: Double) {
        lastPowerConsumption[host] = draw
    }

    override fun reportHostSlice(
        time: Long,
        requestedBurst: Long,
        grantedBurst: Long,
        overcommissionedBurst: Long,
        interferedBurst: Long,
        cpuUsage: Double,
        cpuDemand: Double,
        numberOfDeployedImages: Int,
        hostServer: Server,
        duration: Long
    ) {
        val previousEvent = currentHostEvent[hostServer]
        when {
            previousEvent == null -> {
                val event = HostEvent(
                    time,
                    5 * 60 * 1000L,
                    hostServer,
                    numberOfDeployedImages,
                    requestedBurst,
                    grantedBurst,
                    overcommissionedBurst,
                    interferedBurst,
                    cpuUsage,
                    cpuDemand,
                    lastPowerConsumption[hostServer] ?: 200.0,
                    hostServer.flavor.cpuCount
                )

                currentHostEvent[hostServer] = event
            }
            previousEvent.timestamp == time -> {
                val event = HostEvent(
                    time,
                    previousEvent.duration,
                    hostServer,
                    numberOfDeployedImages,
                    requestedBurst,
                    grantedBurst,
                    overcommissionedBurst,
                    interferedBurst,
                    cpuUsage,
                    cpuDemand,
                    lastPowerConsumption[hostServer] ?: 200.0,
                    hostServer.flavor.cpuCount
                )

                currentHostEvent[hostServer] = event
            }
            else -> {
                processHostEvent(previousEvent)

                val event = HostEvent(
                    time,
                    time - previousEvent.timestamp,
                    hostServer,
                    numberOfDeployedImages,
                    requestedBurst,
                    grantedBurst,
                    overcommissionedBurst,
                    interferedBurst,
                    cpuUsage,
                    cpuDemand,
                    lastPowerConsumption[hostServer] ?: 200.0,
                    hostServer.flavor.cpuCount
                )

                currentHostEvent[hostServer] = event
            }
        }
    }

    private var hostAggregateMetrics: AggregateHostMetrics = AggregateHostMetrics()
    private val hostMetrics: MutableMap<Server, HostMetrics> = mutableMapOf()

    private fun processHostEvent(event: HostEvent) {
        val slices = event.duration / SLICE_LENGTH

        hostAggregateMetrics = AggregateHostMetrics(
            hostAggregateMetrics.totalRequestedBurst + event.requestedBurst,
            hostAggregateMetrics.totalGrantedBurst + event.grantedBurst,
            hostAggregateMetrics.totalOvercommittedBurst + event.overcommissionedBurst,
            hostAggregateMetrics.totalInterferedBurst + event.interferedBurst,
            hostAggregateMetrics.totalPowerDraw + (slices * (event.powerDraw / 12)),
            hostAggregateMetrics.totalFailureSlices + if (event.host.state != ServerState.ACTIVE) slices.toLong() else 0,
            hostAggregateMetrics.totalFailureVmSlices + if (event.host.state != ServerState.ACTIVE) event.vmCount * slices.toLong() else 0
        )

        hostMetrics.compute(event.host) { key, prev ->
            HostMetrics(
                (event.cpuUsage.takeIf { event.host.state == ServerState.ACTIVE } ?: 0.0) + (prev?.cpuUsage ?: 0.0),
                (event.cpuDemand.takeIf { event.host.state == ServerState.ACTIVE } ?: 0.0) + (prev?.cpuDemand ?: 0.0),
                event.vmCount + (prev?.vmCount ?: 0),
                1 + (prev?.count ?: 0)
            )
        }
    }

    private val SLICE_LENGTH: Long = 5 * 60 * 1000

    public data class AggregateHostMetrics(
        val totalRequestedBurst: Long = 0,
        val totalGrantedBurst: Long = 0,
        val totalOvercommittedBurst: Long = 0,
        val totalInterferedBurst: Long = 0,
        val totalPowerDraw: Double = 0.0,
        val totalFailureSlices: Long = 0,
        val totalFailureVmSlices: Long = 0,
    )

    public data class HostMetrics(
        val cpuUsage: Double,
        val cpuDemand: Double,
        val vmCount: Long,
        val count: Long
    )

    private var provisionerMetrics: AggregateProvisionerMetrics = AggregateProvisionerMetrics()

    override fun reportProvisionerMetrics(time: Long, event: VirtProvisioningEvent.MetricsAvailable) {
        provisionerMetrics = AggregateProvisionerMetrics(
            max(event.totalVmCount, provisionerMetrics.vmTotalCount),
            max(event.waitingVmCount, provisionerMetrics.vmWaitingCount),
            max(event.activeVmCount, provisionerMetrics.vmActiveCount),
            max(event.inactiveVmCount, provisionerMetrics.vmInactiveCount),
            max(event.failedVmCount, provisionerMetrics.vmFailedCount),
        )
    }

    public data class AggregateProvisionerMetrics(
        val vmTotalCount: Int = 0,
        val vmWaitingCount: Int = 0,
        val vmActiveCount: Int = 0,
        val vmInactiveCount: Int = 0,
        val vmFailedCount: Int = 0
    )

    override fun close() {
        for ((_, event) in currentHostEvent) {
            processHostEvent(event)
        }
        currentHostEvent.clear()
    }

    public fun getResult(): Result {
        return Result(
            hostAggregateMetrics.totalRequestedBurst,
            hostAggregateMetrics.totalGrantedBurst,
            hostAggregateMetrics.totalOvercommittedBurst,
            hostAggregateMetrics.totalInterferedBurst,
            hostMetrics.map { it.value.cpuUsage / it.value.count }.average(),
            hostMetrics.map { it.value.cpuDemand / it.value.count }.average(),
            hostMetrics.map { it.value.vmCount.toDouble() / it.value.count }.average(),
            hostMetrics.map { it.value.vmCount.toDouble() / it.value.count }.maxOrNull() ?: 0.0,
            hostAggregateMetrics.totalPowerDraw,
            hostAggregateMetrics.totalFailureSlices,
            hostAggregateMetrics.totalFailureVmSlices,
            provisionerMetrics.vmTotalCount,
            provisionerMetrics.vmWaitingCount,
            provisionerMetrics.vmInactiveCount,
            provisionerMetrics.vmFailedCount,
        )
    }

    public data class Result(
        public val totalRequestedBurst: Long,
        public val totalGrantedBurst: Long,
        public val totalOvercommittedBurst: Long,
        public val totalInterferedBurst: Long,
        public val meanCpuUsage: Double,
        public val meanCpuDemand: Double,
        public val meanNumDeployedImages: Double,
        public val maxNumDeployedImages: Double,
        public val totalPowerDraw: Double,
        public val totalFailureSlices: Long,
        public val totalFailureVmSlices: Long,
        public val totalVmsSubmitted: Int,
        public val totalVmsQueued: Int,
        public val totalVmsFinished: Int,
        public val totalVmsFailed: Int
    )
}
