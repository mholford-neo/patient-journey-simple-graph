package org.mholford.neo4j

import org.mitre.synthea.engine.Generator
import org.mitre.synthea.helpers.Config
import java.nio.file.Path

class SyntheaBuilder(private val population: Int, private val seed: Long) {
  
  /* Create sample patient via Synthea.  Output only the CSV files we are interested in */
  fun generateData() {
    val options = Generator.GeneratorOptions().also { opt ->
      opt.seed = seed
      opt.population = population
    }
    Config.set("exporter.csv.export", "true")
    Config.set("exporter.fhir.export", "false")
    Config.set("exporter.hospital.fhir.export", "false")
    Config.set("exporter.practitioner.fhir.export", "false")
    Config.set("exporter.csv.included_files", "patients.csv,encounters.csv,conditions.csv")
    val generator = Generator(options)
    generator.run()
  }
  
  fun transformData(outputFolder: String) {
    SyntheaGraph(Path.of("output", "csv")).produceSimpleModel(Path.of(outputFolder))
  }
}

fun main(args: Array<String>) {
  val population = args[0]
  val seed = when {
    args.size >= 2 -> args[1].toLong()
    else ->  System.currentTimeMillis()
  }
  SyntheaBuilder(population.toInt(), seed).apply {
    generateData()
    transformData("output/import")
  }
}