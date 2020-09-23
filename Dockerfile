FROM openjdk:8

RUN apt-get update --fix-missing
RUN apt-get install -y net-tools \
                   dnsutils \
                   supervisor \
                   bash \
                   maven

# Bundle app source
COPY . /src
WORKDIR /src

# Compile java
RUN mvn compile

# Add supervisord config
RUN mkdir /var/logs
RUN touch /var/logs/supervisord.log
RUN chmod a+rw /var/logs/supervisord.log
ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD /usr/bin/supervisord --nodaemon -c /etc/supervisor/conf.d/supervisord.conf