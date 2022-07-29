1.	Take data coordinate data from Bird Eye Coordinate Dashboard
2.	Take last 31 day sales data by below query.
3.	Download the latest PBF (Protocolbuffer Binary Format) file of the state where the unit operates from this link. Eg., odisha-latest.osm.pbf
4.	Upload the PBF file to the docker container of the Open Route Service Engine by following steps:-<br/> 
  a.	Take pull from this git link: https://github.com/GIScience/openrouteservice.git (If not taken)<br/> 
  b.	Go to docker-compose.yml file and set BUILD_GRAPHS=True<br/> 
  c.	Copy the downloaded PBF file to openrouteservice\docker\data\odisha-latest.osm.pbf<br/> 
  d.	Go to openrouteservice\docker\conf\ors-config.json and in routing source provide the path of the above pbf file like "data/odisha-latest.osm.pbf"<br/> 
  e.	Install docker by ->  sudo apt  install docker.io<br/> 
  f.	Check docker service running or not by – systsemctl status docker<br/> 
  g.	Install docker-compose – sudo apt install docker-compose<br/>
  h.	Go to docker directory of the project and execute following commands.<br/>
    i.	mkdir -p conf elevation_cache graphs logs/ors logs/tomcat<br/>
    ii.	docker-compose up -d<br/>
5.  Copy pbf file to docker container<br/>
  a.	Windows<br/>
    i.	docker cp "D:\code\ascent\route optimization\Engines\ors\openrouteservice\docker\data\odisha-latest.osm.pbf" 899da61832ec:"/ors-core/data/odisha-latest.osm.pbf"<br/>
  b.	Linux<br/>
    i.	sudo docker cp "data/bihar-latest.osm.pbf" 4e39a50b616a:"/ors-core/data/bihar-latest.osm.pbf"<br/>
    ii.	Check container status -> sudo docker container stats 4e39a50b616a<br/>
    iii.	Stop container and start container -><br/>
      *-	sudo docker container stop 4e39a50b616a*<br/>
      *-	sudo docker container start 4e39a50b616a*<br/>

**Supporting CMDs**:
1.	sudo docker exec -t -i 4e39a50b616a /bin/bash
