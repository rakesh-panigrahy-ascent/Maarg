1.	Take data coordinate data from Bird Eye Coordinate Dashboard
2.	Take last 31 day sales data by below query.
3.	Download the latest PBF (Protocolbuffer Binary Format) file of the state where the unit operates from this link. Eg., odisha-latest.osm.pbf
4.	Upload the PBF file to the docker container of the Open Route Service Engine by following steps:-
a.	Take pull from this git link: https://github.com/GIScience/openrouteservice.git (If not taken)
b.	Go to docker-compose.yml file and set BUILD_GRAPHS=True
c.	Copy the downloaded PBF file to openrouteservice\docker\data\odisha-latest.osm.pbf
d.	Go to openrouteservice\docker\conf\ors-config.json and in routing source provide the path of the above pbf file like "data/odisha-latest.osm.pbf"
e.	Install docker by ->  sudo apt  install docker.io
f.	Check docker service running or not by – systsemctl status docker
g.	Install docker-compose – sudo apt install docker-compose
h.	Go to docker directory of the project and execute following commands.
i.	mkdir -p conf elevation_cache graphs logs/ors logs/tomcat
ii.	docker-compose up -d
i.	Copy pbf file to docker container
i.	Windows
1.	docker cp "D:\code\ascent\route optimization\Engines\ors\openrouteservice\docker\data\odisha-latest.osm.pbf" 899da61832ec:"/ors-core/data/odisha-latest.osm.pbf"
ii.	Linux 
1.	sudo docker cp "data/bihar-latest.osm.pbf" 4e39a50b616a:"/ors-core/data/bihar-latest.osm.pbf"
2.	Check container status -> sudo docker container stats 4e39a50b616a
3.	Stop container and start container ->
a.	sudo docker container stop 4e39a50b616a
b.	sudo docker container start 4e39a50b616a
Supporting CMDs:
1.	sudo docker exec -t -i 4e39a50b616a /bin/bash
