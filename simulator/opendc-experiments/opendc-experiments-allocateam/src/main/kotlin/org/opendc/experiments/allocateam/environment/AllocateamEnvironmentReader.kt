package org.opendc.experiments.allocateam.environment

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.CoroutineScope
import org.opendc.compute.core.metal.service.ProvisioningService
import org.opendc.compute.core.metal.service.SimpleProvisioningService
import org.opendc.compute.simulator.SimBareMetalDriver
import org.opendc.compute.simulator.power.LinearLoadPowerModel
import org.opendc.core.Environment
import org.opendc.core.Platform
import org.opendc.core.Zone
import org.opendc.core.services.ServiceRegistry
import org.opendc.format.environment.EnvironmentReader
import org.opendc.simulator.compute.SimMachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import java.io.InputStream
import java.time.Clock
import java.util.*

/**
 * A parser for the JSON experiment setup files used for the SC18 paper: "A Reference Architecture for Topology
 * Schedulers".
 *
 * @param input The input stream to read from.
 * @param mapper The Jackson object mapper to use.
 */
public class AllocateamEnvironmentReader(input: InputStream, mapper: ObjectMapper = jacksonObjectMapper()) : EnvironmentReader {
    /**
     * The environment that was read from the file.
     */
    private val setup: Setup = mapper.readValue(input)

    override suspend fun construct(coroutineScope: CoroutineScope, clock: Clock): Environment {
        var counter = 0
        val nodes = setup.rooms.flatMap { room ->
            room.objects.flatMap { roomObject ->
                when (roomObject) {
                    is RoomObject.Rack -> {
                        roomObject.machines.map { machine ->
                            val cores = machine.cpus.flatMap { id ->
                                when (id) {
                                    1 -> {
                                        val node = ProcessingNode("Intel", "Core(TM) i7-6920HQ", "amd64", 4)
                                        List(node.coreCount) { ProcessingUnit(node, it, 4100.0) }
                                    }
                                    2 -> {
                                        val node = ProcessingNode("Intel", "Core(TM) i7-6920HQ", "amd64", 2)
                                        List(node.coreCount) { ProcessingUnit(node, it, 3500.0) }
                                    }
                                    else -> throw IllegalArgumentException("The cpu id $id is not recognized")
                                }
                            }
                            SimBareMetalDriver(
                                coroutineScope,
                                clock,
                                UUID.randomUUID(),
                                "node-${counter++}",
                                emptyMap(),
                                SimMachineModel(cores, listOf(MemoryUnit("", "", 2300.0, 16000))),

                                // For now we assume a simple linear load model with an idle draw of ~200W and a maximum
                                // power draw of 350W.
                                // Source: https://stackoverflow.com/questions/6128960
                                powerModel = LinearLoadPowerModel(200.0, 350.0)
                            )
                        }
                    }
                }
            }
        }

        val provisioningService = SimpleProvisioningService()
        for (node in nodes) {
            provisioningService.create(node)
        }

        val serviceRegistry = ServiceRegistry().put(ProvisioningService, provisioningService)
        val platform = Platform(
            UUID.randomUUID(),
            "allocateam-platform",
            listOf(
                Zone(UUID.randomUUID(), "zone", serviceRegistry)
            )
        )

        return Environment(setup.name, null, listOf(platform))
    }

    override fun close() {}
}
