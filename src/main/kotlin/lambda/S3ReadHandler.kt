package lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.S3Event

class S3ReadHandler : RequestHandler<S3Event, String> {
    override fun handleRequest(input: S3Event?, context: Context?): String {

        val record = input!!.records.first()

        val executor = ExecutorLambda(record.s3.bucket.name, record.s3.`object`.key)
        executor.exec()
        return "Ok"
    }
}