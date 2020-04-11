FROM openjdk:12-oraclelinux7

RUN yum -y install \
    libXi \
   libXrender

RUN yum -y install libXtst
COPY . /usr/app/
WORKDIR /usr/app/

CMD java -jar cs1660-project.jar