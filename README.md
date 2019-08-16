# Verkeersborden harvester
Harvests verkeersbordendata, based on (manually collected) data from [wegcode.be](https://wegcode.be/wetteksten/secties/kb/wegcode/248-art65)

## Running
```docker run -it --rm --name verkeersborden-harvester -v "$PWD":/app -w /app ruby:2.5  /bin/bash run.sh```

## Output
In output folder.
* Codelists for https://data.vlaanderen.be/ns/mobiliteit#Verkeersbordconcept, https://data.vlaanderen.be/ns/mobiliteit#Vekeersbordcategorie, https://data.vlaanderen.be/ns/mobiliteit#VerkeersbordconceptstatusCode
* Verkeersborden along with image data ready to be loaded in to a mu.semte.ch application with file service enabled. You need only to ingest this file (codelists are there)
