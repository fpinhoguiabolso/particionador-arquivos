package lambda

import s3.reader.Reader
import s3.writer.Writer
import util.Partitioner
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis

class ExecutorLambda(val butketNameOrigin: String, val key: String) {

    val bucketNameDestiny = "tombamento-destiny"
    val size = 1_000_000

    fun exec () {
        val time = measureTimeMillis {

            val reader = Reader(butketNameOrigin)

            val file = reader.loadFile(key)

            val partitioner = Partitioner()
            val contents = partitioner.exec(file, size)

            val prefixFile = file.nome.replace(".csv", "")
            val writer = Writer(bucketNameDestiny)
            writer.exec(prefixFile, contents)
        }
        println("Tempo de execução: ${TimeUnit.MILLISECONDS.toSeconds(time)}")
    }
}