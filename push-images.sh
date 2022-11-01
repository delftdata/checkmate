docker tag dev/universalis-coordinator:latest kpsarakis/universalis-coordinator:latest
docker push kpsarakis/universalis-coordinator:latest

docker tag dev/universalis:latest kpsarakis/universalis-worker:latest
docker push kpsarakis/universalis-worker:latest