# https://hub.docker.com/layers/library/node/22.11.0-bookworm/images/sha256-748cfe6211ab7422bd4a6597e93e45355e8e6baf5d4707887843522a0c6ea860?context=explore
FROM node:22.11.0-bookworm@sha256:ec878c763e9fad09d22aae86e2edcb7a05b397dfe8411c16e2b90158d595e2ce

RUN apt update && apt upgrade -y

EXPOSE 3000

WORKDIR /app

CMD ["/bin/bash"]
