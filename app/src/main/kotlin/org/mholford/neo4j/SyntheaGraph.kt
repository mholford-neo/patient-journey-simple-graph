package org.mholford.neo4j

import okio.BufferedSink
import okio.buffer
import okio.sink
import java.nio.file.Path

/* Various string constants */
const val NEW_LINE = "\n"
const val COMMA = ","
const val PATIENT_LABEL = "Patient"
const val ENCOUNTER_LABEL = "Encounter"
const val CONDITION_LABEL = "Condition"
const val PATIENT_ENCOUNTER_REL = "HAS_ENCOUNTER"
const val ENCOUNTER_CONDITION_REL = "FOUND_CONDITION"
const val ENCOUNTER_NEXT_REL = "NEXT"
const val PATIENTS_FILE = "patients.csv"
const val ENCOUNTERS_FILE = "encounters.csv"
const val CONDITIONS_FILE = "conditions.csv"
const val PATIENT_NODE_FILE = "node_patients.csv"
const val ENCOUNTER_NODE_FILE = "node_encounters.csv"
const val CONDITION_NODE_FILE = "node_conditions.csv"
const val PATIENT_ENCOUNTER_REL_FILE = "rel_patient_encounter.csv"
const val ENCOUNTER_CONDITION_REL_FILE = "rel_encounter_conditions.csv"
const val ENCOUNTER_NEXT_REL_FILE = "rel_encounter_next.csv"

/* A simple representation of a Synthea patient */
data class Patient(
  val id: String, val birthDate: String, val deathDate: String, val firstName: String,
  val lastName: String, val gender: String, val city: String,
  val encounters: MutableMap<String, Encounter>
)

/* Simplified representation of a Synthea encounter.  Only one type of event (Condition) can occur
* in the Encounter */
data class Encounter(val id: String, val date: String, val encClass: String, var condition: Condition?)

/* Simplified representation of a Synthea Condition */
data class Condition(val id: String, val description: String)


/* Class handles reading the input files; processing them
* and outputting them to CSV files */
class SyntheaGraph(private val inputFolder: Path) {
  
  /* Basic implementation.  All patient data is read into memory before outputing csv files */
  fun produceSimpleModel(outputFolder: Path) {
    val writers = initWriters(outputFolder)
    val patientMap = mutableMapOf<String, Patient>()
    val conditionMap = mutableMapOf<String, Condition>()
    
    readFile(inputFolder.resolve(PATIENTS_FILE), 1) { line ->
      val ss = line.split(",")
      patientMap[ss[0]] = Patient(ss[0], ss[1], ss[2], ss[7], ss[8], ss[14], ss[17], mutableMapOf())
    }
    
    readFile(inputFolder.resolve(ENCOUNTERS_FILE), 1) { line ->
      val ss = line.split(",")
      val patientId = ss[3]
      val patient = patientMap[patientId]
      val encounterId = ss[0]
      patient?.encounters?.put(encounterId, Encounter(encounterId, ss[1], ss[7], null))
    }
    
    readFile(inputFolder.resolve(CONDITIONS_FILE), 1) { line ->
      val ss = line.split(",")
      val patientId = ss[2]
      val encounterId = ss[3]
      val conditionId = ss[4]
      val condition = Condition(conditionId, ss[5])
      conditionMap[conditionId] = condition
      patientMap[patientId]?.encounters?.get(encounterId)?.condition = condition
    }
    
    writers[CONDITION_NODE_FILE]?.let { w ->
      conditionMap.forEach { (_, condition) ->
        writeConditionNode(w, condition)
      }
    }
    patientMap.forEach { (_, patient) ->
      writers[PATIENT_NODE_FILE]?.let { w ->
        writePatientNode(w, patient)
      }
      
      /* Get Encounter for this patient and sort them by date */
      val ptEncs = patient.encounters.values.filter { it.condition != null }.sortedBy { it.date }
      
      var prevEncounter: String? = null
      ptEncs.forEach { encounter ->
        encounter.condition?.let { condition ->
          writers[ENCOUNTER_NODE_FILE]?.let { w ->
            writeEncounterNode(w, encounter)
          }
          writers[PATIENT_ENCOUNTER_REL_FILE]?.let { w ->
            writeRel(w, patient.id, encounter.id, PATIENT_ENCOUNTER_REL)
          }
          writers[ENCOUNTER_CONDITION_REL_FILE]?.let { w ->
            writeRel(w, encounter.id, condition.id, ENCOUNTER_CONDITION_REL)
          }
          prevEncounter?.let { prevEncounter ->
            writers[ENCOUNTER_NEXT_REL_FILE]?.let { w ->
              writeRel(w, prevEncounter, encounter.id, ENCOUNTER_NEXT_REL)
            }
          }
        }
        
        prevEncounter = encounter.id
      }
    }
    writers.values.forEach { sink -> sink.close() }
  }
  
  /**
   * Create an output for each file we are generating.  Also write the appropriate header for that file.
   * Return a map of each Sink to its name
   */
  private fun initWriters(folder: Path): Map<String, BufferedSink> {
    folder.toFile().mkdirs()
    val out = mutableMapOf<String, BufferedSink>()
    mapOf(
      PATIENT_NODE_FILE to "id:ID,birthDate:datetime,deathDate:datetime,firstName,lastName,gender,city,:LABEL\n",
      ENCOUNTER_NODE_FILE to "id:ID,date:datetime,encClass,:LABEL\n",
      CONDITION_NODE_FILE to "id:ID,name,:LABEL\n",
      ENCOUNTER_CONDITION_REL_FILE to ":START_ID,:END_ID,:TYPE\n",
      PATIENT_ENCOUNTER_REL_FILE to ":START_ID,:END_ID,:TYPE\n",
      ENCOUNTER_NEXT_REL_FILE to ":START_ID,:END_ID,:TYPE\n"
    ).forEach { e ->
      val file = e.key
      val header = e.value
      val buf = folder.resolve(file).sink().buffer()
      buf.writeUtf8(header)
      out[file] = buf
    }
    
    return out
  }
  
  private fun writeConditionNode(writer: BufferedSink, condition: Condition) {
    writeNode(writer, listOf(condition.id, condition.description), CONDITION_LABEL)
  }
  
  private fun writeEncounterNode(writer: BufferedSink, encounter: Encounter) {
    writeNode(writer, listOf(encounter.id, encounter.date, encounter.encClass), ENCOUNTER_LABEL)
  }
  
  private fun writePatientNode(writer: BufferedSink, patient: Patient) {
    writeNode(
      writer, listOf(
        patient.id, patient.birthDate, patient.deathDate, patient.firstName, patient.lastName, patient.gender,
        patient.city
      ), PATIENT_LABEL
    )
  }
  
  private fun writeRel(writer: BufferedSink, startId: String, endId: String, relName: String) {
    with(writer) {
      writeUtf8(startId).writeUtf8(COMMA).writeUtf8(endId).writeUtf8(COMMA).writeUtf8(relName)
      writeUtf8(NEW_LINE)
    }
  }
  
  private fun writeNode(writer: BufferedSink, attributes: List<String>, label: String) {
    with(writer) {
      attributes.forEach { attr ->
        writeUtf8(attr).writeUtf8(COMMA)
      }
      writeUtf8(label).writeUtf8(NEW_LINE)
    }
  }
}