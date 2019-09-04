package s3.reader

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import java.io.InputStreamReader

class Reader(val bucketName: String) {

    fun loadFile(key: String): Content {
        println("Start Load file $key")
        val s3Client = AmazonS3ClientBuilder.defaultClient()
        val file = s3Client.getObject(bucketName, key)
        println("Finish Load file $key")
        return Content(file.key, file.objectContent.reader())
    }
}

data class Content(val nome:String, val content:InputStreamReader)
