package s3.writer

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.*
import util.Contents
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.util.*

class Writer(val bucketName:String) {

    fun exec(prefixFile: String, contents: Contents) {

        println("Start writer S3 - ${bucketName}")

        val s3Client = AmazonS3ClientBuilder.defaultClient()

        for(particao in contents.content) {

            val outputStream = ByteArrayOutputStream()
            outputStream.write(contents.header.plus("\n").toByteArray())
            for(line in particao) {
                outputStream.write(line.plus("\n").toByteArray())
            }
            val bytes = outputStream.toByteArray()

            val putObjectRequest = PutObjectRequest(bucketName, generateNameKey(prefixFile), ByteArrayInputStream(bytes), metadata(bytes))
            s3Client.putObject(putObjectRequest)
            outputStream.close()
        }
        println("Finish writer S3 - ${bucketName}")
    }

    private fun generateNameKey(prefixFile: String): String {
        val data = SimpleDateFormat("ddMMyyyyhhmmss").format(Date())
        return "${prefixFile}_$data.csv"
    }

    private fun metadata(bytes: ByteArray): ObjectMetadata {
        val metadata = ObjectMetadata()
        metadata.contentType = "application/csv"
        metadata.contentLength = bytes.size.toLong()
        return metadata
    }
}