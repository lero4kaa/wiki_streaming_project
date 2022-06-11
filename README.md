# Big Data project: WIKI stream
Project for Big Data course at Ukrainian Catholic University: processing and preparing data from wikipedia stream in order to provide two sets of APIs
to user.

## Project architecture
![](images/arch.png)

## Instructions:
```
docker-compose build && docker-compose up
```
*Warning: the docker image we use takes a lot of time to be downloaded (from 10 to 30 minutes).*

Connect to the website on [http://0.0.0.0:5000](http://0.0.0.0:5000). (here You will 
be able to send requests for both types of API sets).

Finish and clean up:
```
docker-compose down
```

## Examples
Examples (after ~8 hours processing and preparing wiki data):
![1](images/example1.png)
![2](images/example2.png)
![3](images/example3.png)
![4](images/example4.png)
![5](images/example5.png)
![6](images/example6.png)
![7](images/example7.png)
![8](images/example8.png)

Home interface:
![interface](images/interface.png)