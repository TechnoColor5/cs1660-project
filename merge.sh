gsutil rm gs://dataproc-staging-us-west1-196681246404-tl4catln/output.txt
gsutil rm gs://dataproc-staging-us-west1-196681246404-tl4catln/output/_SUCCESS
hadoop fs -getmerge gs://dataproc-staging-us-west1-196681246404-tl4catln/output ./output.txt
hadoop fs -copyFromLocal ./output.txt
hadoop fs -cp ./output.txt gs://dataproc-staging-us-west1-196681246404-tl4catln/output.txt
hadoop fs -rm ./output.txt
gsutil rm gs://dataproc-staging-us-west1-196681246404-tl4catln/output