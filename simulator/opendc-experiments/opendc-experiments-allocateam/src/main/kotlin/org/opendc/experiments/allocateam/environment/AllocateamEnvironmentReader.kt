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
 * A topology setup.
 *
 * @property name The name of the setup.
 * @property rooms The rooms in the topology.
 */
internal data class Setup(val name: String, val totalMachines: Int)

/**
 * A parser for the JSON experiment setup files used for the Allocateam experiment.
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
        val nodes = (0 until setup.totalMachines).map { id ->
            val node = ProcessingNode("Intel", "Core(TM) i7-6920HQ", "amd64", 2)
            val cores = List(node.coreCount) { ProcessingUnit(node, it, 3500.0) }

            SimBareMetalDriver(
                coroutineScope,
                clock,
                UUID.randomUUID(),
                "node-${id}",
                emptyMap(),
                SimMachineModel(cores, listOf(MemoryUnit("", "", 2300.0, 16000))),

                // For now we assume a simple linear load model with an idle draw of ~200W and a maximum
                // power draw of 350W.
                // Source: https://stackoverflow.com/questions/6128960
                powerModel = LinearLoadPowerModel(200.0, 350.0)
            )
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
