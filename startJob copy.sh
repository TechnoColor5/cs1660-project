#!/bin/bash/
echo "starting"
gcloud dataproc jobs submit hadoop --cluster=cluster-truck \
	 --region=us-west1 \
	 --jar=gs://dataproc-staging-us-west1-196681246404-tl4catln/jar/invertedindexjob.jar \
	 --project=cs1660-273518 \
	 -- InvertedIndexJob gs://dataproc-staging-us-west1-196681246404-tl4catln/Data/Hugo/ gs://dataproc-staging-us-west1-196681246404-tl4catln/output70
echo "finished"