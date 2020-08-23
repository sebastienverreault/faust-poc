# Fresh machine



apt-get update  # updates the package index cache
apt-get upgrade -y  # updates packages


# installs system tools
apt-get install -y bzip2 gcc git  # system tools
apt-get install -y htop screen vim wget  # system tools
apt install make
apt install openjdk-14-jre-headless        
apt-get upgrade -y bash  # upgrades bash if necessary
apt-get clean  # cleans up the package index cache



# Install docker and compose?
apt  install docker.io 
apt  install docker-compose 




# INSTALL MINICONDA
# downloads Miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O Miniconda.sh
bash Miniconda.sh -b  # installs it
rm -rf Miniconda.sh  # removes the installer
export PATH="/root/miniconda3/bin:$PATH"  # prepends the new path

# Remove Miniconda
#rm -rf ~/miniconda3
#rm -rf /root/miniconda3





# INSTALL MINICONDA
# downloads Miniconda
wget https://repo.anaconda.com/archive/Anaconda3-2020.07-Linux-x86_64.sh -O Anaconda.sh
bash Anaconda.sh -b  # installs it
rm -rf Anaconda.sh  # removes the installer
export PATH="/root/anaconda3/bin:$PATH"  # prepends the new path


# Faust
#conda env remove --name elk-poc
conda create -n elk-poc python=3.6
conda activate elk-poc
pip install -r requirements.txt
pip install -U faust

#
conda create -n faust-poc python=3.8
conda activate faust-poc
pip install -r requirements.txt


# INSTALL PYTHON LIBRARIES
pip install pandas
pip install numpy
pip install scipy
pip install matplotlib
pip install seaborn
pip install statsmodels
pip install ipython



# Download and unpak kafka for the scripts
wget http://mirror.cogentco.com/pub/apache/kafka/2.6.0/kafka_2.13-2.6.0.tgz
curl "http://mirror.cogentco.com/pub/apache/kafka/2.6.0/kafka_2.13-2.6.0.tgz" -o ~/downloads/kafka.tgz
mkdir ~/kafka && cd ~/kafka
tar -xvzf ~/downloads/kafka.tgz --strip 1


# Listen for kafka traffic
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic input --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic colors_topic --from-beginning

# Produce kafka traffic
~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input


# Listen for kafka traffic
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weblogs_stream --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weblogs_tokenized_stream --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weblogs_persistence_stream --from-beginning
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic weblogs_stats_stream --from-beginning


# Run the kafka docker-compose
make kafka


# Run the kafka with elasticsearch docker-compose
mkdir kafka_es
make elk


# Test elasticsearch is alive
curl -X GET "localhost:9200/_cat/nodes?v&pretty"

# ??
# docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.8.12
# docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.9.0

# max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]
vi /etc/sysctl.conf
# ++ > vm.max_map_count=262144
sudo sysctl -p


# CUSTOMIZATION
cd /root/
wget http://hilpisch.com/.vimrc  # Vim configuration

