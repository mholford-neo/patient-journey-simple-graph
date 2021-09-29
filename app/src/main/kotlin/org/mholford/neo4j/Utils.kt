package org.mholford.neo4j

import okio.Source
import okio.buffer
import okio.source
import java.nio.file.Path

typealias StringOp = (String) -> Unit

/**
 * Stream lines from input Source and perform subsequent
 * closure on each.
 */
fun Source.streamLines(skipLines: Int, work: StringOp) {
  var linesRead = 0
  this.buffer().use { buf ->
    while (true) {
      val line = buf.readUtf8Line() ?: break
      if (linesRead++ > skipLines) {
        work(line)
      }
    }
  }
}

/**
 * Open the specified file and perform subsequent closure on each
 * line of the file.
 */
fun readFile(filePath: Path, skipLines: Int = 0, work: StringOp) {
  filePath.source().use { src ->
    src.streamLines(skipLines, work)
  }
}