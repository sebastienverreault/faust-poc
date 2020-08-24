#
# From a fresh machine
apt-get update  # updates the package index cache
apt-get upgrade -y  # updates packages


#
# installs system tools
apt-get install -y bzip2 gcc git  # system tools
apt-get install -y htop screen vim wget  # system tools
apt install make
apt install openjdk-14-jre-headless        
apt-get upgrade -y bash  # upgrades bash if necessary
apt-get clean  # cleans up the package index cache


#
# Install docker and compose?
apt  install docker.io 
apt  install docker-compose 


#
# Install Miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O Miniconda.sh
bash Miniconda.sh -b  # installs it
rm -rf Miniconda.sh  # removes the installer
export PATH="/root/miniconda3/bin:$PATH"  # prepends the new path

# 
# In case it's needed... to remove Miniconda
#rm -rf ~/miniconda3
#rm -rf /root/miniconda3


#
# Alternative is to get the full Anaconda
wget https://repo.anaconda.com/archive/Anaconda3-2020.07-Linux-x86_64.sh -O Anaconda.sh
bash Anaconda.sh -b  # installs it
rm -rf Anaconda.sh  # removes the installer
export PATH="/root/anaconda3/bin:$PATH"  # prepends the new path

#
# Virtual Python Env. with Faust & co on 3.8
conda create -n faust-poc python=3.8
conda activate faust-poc
pip install -r requirements.txt


#
# Download and unpak kafka for the scripts
wget http://mirror.cogentco.com/pub/apache/kafka/2.6.0/kafka_2.13-2.6.0.tgz
curl "http://mirror.cogentco.com/pub/apache/kafka/2.6.0/kafka_2.13-2.6.0.tgz" -o ~/downloads/kafka.tgz
mkdir ~/kafka && cd ~/kafka
tar -xvzf ~/downloads/kafka.tgz --strip 1


#
# Ref. for looking/generating Kafka traffic - test app to validate the infra
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic input --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic colors_topic --from-beginning

#
# Produce Kafka traffic - test app to validate the infra
~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input

#
# Weblogs streaming app topics
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weblogs --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weblogs_tokens --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weblogs_stats_stream --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weblogs_persistence_stream --from-beginning


#
# Run the kafka docker-compose to "power-up" the infra
make kafka-elk

#
# ... to take it down and cleanup
make kafka-elk-down

#
# Run the ElasticSearch app aka Weblogs streaming
make run-es-app
# In debug
make run-es-app-debug

#
# Standlalone app to make sure nothing is broken
make run-standalone-app:
make run-standalone-app-debug:

#
# Test elasticsearch is alive
curl -X GET "localhost:9200/_cat/nodes?v&pretty"


#
# Tricky setings with es? bootstrap checks
#
# max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]
vi /etc/sysctl.conf
# ++ > vm.max_map_count=262144
sudo sysctl -p


