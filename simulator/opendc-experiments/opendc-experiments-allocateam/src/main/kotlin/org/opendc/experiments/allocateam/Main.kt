package org.opendc.experiments.allocateam

import com.github.ajalt.clikt.core.CliktCommand
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

public class ExperimentCli : CliktCommand(name = "allocateam") {
    override fun run() {
        logger.info { "Hello world" }
    }
}

public fun main(args: Array<String>): Unit = ExperimentCli().main(args)
