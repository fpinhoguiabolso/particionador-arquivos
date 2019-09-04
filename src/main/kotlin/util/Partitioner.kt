package util

import s3.reader.Content
import java.io.BufferedReader

class Partitioner {

    fun exec(file: Content, size: Int):Contents {

        println("Start partitioner arquivos")

        val bufferedReader = BufferedReader(file.content)

        var content = bufferedReader.readLines()
        val header = content.first()
        content = content.drop(1)
        val listas = content.chunked(size)
        bufferedReader.close()
        println("Finish partitioner arquivos")
        return Contents(header, listas)
    }
}

data class Contents(val header:String, val content: List<List<String>>)