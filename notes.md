# Notes

## Docker 

Command to run the hbase server.  `--net host` is used so that the open ports are on localhost instead of on the bridged network. The reason for this is that java can talk to zookeeper on 172.17.0.2 (the bridged network) but it will then return the hostname of the hbase region server which is not visible outside the container. An alternative may be to edit /etc/hosts to point the name of the container host to the 172.17.0.2.
```bash
docker run --net host --name hbasePheonix -h hbase nerdammer/hbaseOB-phoenix:1.1.0.1-4.4.0
```
## Zookeeper

To connect to zookeeper in the container download the appropriate version of Zookeeper 

```bash
cd tmp
mkdir zk
cd zk
mv ~/Downloads/zookeeper-3.4.6.tar.gz .
tar -xvf zookeeper-3.4.6.tar.gz
cd zookeeper-3.4.6
cd bin
./zkCli.sh -server 172.17.0.2:2181
```

In the Zookeeper shell you can do commands like:

```
help
ls /
ls /hbase
```
