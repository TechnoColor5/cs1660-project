#!/bin/bash/
echo "starting"
curl -d "hugo.json" -X POST https://dataproc.googleapis.com/v1/projects/cs1660-273518/regions/us-west-1/jobs:submit?key=AIzaSyB10KLU3Iroj-vef0EohrK8tu-DlRAOtSc
echo "finished"