Usage:

to start the service build the jar-
mvn clean install package

execute jar-
java -jar {jar name}

Configuration parameters can be defined in Config.java
OR
Pass these arguments to overrride config parameters
nettyport={port on which netty server needs to be started}
tarantoolPort={port on which tarantool server is listening}
address={address/ur/host where tarantool instance is running}
username={username to connect to tarantool instance}
password={password to connect to tarantool instance}
		
To override adavertiser/campaignId/balanceDate
http://localhost:9090/?advertiserId=111&campaignId=222&balanceDate=333


to specify specific log name-
http://localhost:9090/?logname=nativeImplResponse


