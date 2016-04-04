# Active DNS Parser

The provided C program makes use of the Apache Avro C library to parse active dns data provided by Georgia Tech's Astrolavos Lab (http://astrolavos.gatech.edu/). For access to the Active DNS data please contact someone in the Astrolavos lab and they will gladly work with you.

Currently the Avro schema expected by the program is as follows:

```json"
{"type":"record",
  "name":"ActiveDns",
  "namespace":"astrolavos.avro",
  "avro.codec": "snappy",
  "fields":[
        {"name":"date","type":"string"},
        {"name":"qname","type":"string"},
        {"name":"qtype","type":"int"},
        {"name":"rdata","type":["string","null"]},
        {"name":"ttl","type":["int","null"]},
        {"name":"authority_ips","type":"string"},
        {"name":"count","type":"long"},
        {"name":"hours","type":"int"},
        {"name":"source","type":"string"},
        {"name":"sensor","type":"string"}]}";
```


### Dependencies

#### Lib Snappy
The avro files are compressed with the Snappy codec so we will need this installed to correctly parse the Avro files. **This is required to be installed before the Avro C library is compiled** or else Snappy support will not be enabled for the Avro libary and the active dns avro files will be unparsable to the library.

On Debian based distributions simply run the following command and you should be good to go.
```shell
sudo apt-get install libsnappy-dev
```

####Avro C Library
To run this program you will need to compile and link against the Avro C library. The code and install instructions can be found [here] (https://github.com/apache/avro/tree/master/lang/c). For help understanding the API I have found [this page] (https://avro.apache.org/docs/current/api/c/) to be helpful.



### Usage

After compiling, simply point the binary to a directory full of Avro part files and the program will digest the files out parse them to stdout.

```shell
./parse /home/estuart/avro_files/
```
*Note: Do not forget the trailing slash for the input directory or the command will fail
