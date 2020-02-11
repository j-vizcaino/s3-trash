# s3-trash

**DANGER**: **this removes all the objects from an S3 bucket. There's no way to recover deleted objects from S3 once this completes**.

**USE AT YOUR OWN RISK**

This is a small tool that lists all the objects versions in an S3 bucket and deletes them.
Listing and deleting happens in parallel, thanks to Go concurrency model.

Deletes happen in bulk operations of at most 1,000 deletes per call.
Multiple connections can be opened to S3 to maximize speed.
