# Covid-19-final-project

Developed a Covid 19 database for my intermediate topics in database course. 
The program interacts with Rabbitmq to recieve a payload of patients to input
into the database. The program then goes through the payload and decides which 
hospital to send the patient to based on the patients status, distance to hospital,
and the beds available by the hospital. Then the hospital id is added to the 
patients file and the file is inserted into mongodb. The user can then go to 
the webrowser and enter url's to access the API's. The program also has a real
time reporting functionality. For every 15 seconds if a zipcode has double the 
amount of positive test cases than the previous 15 seconds the zipcode is put into
an alert status and added to the alert list. If the zipcode is less than double the 
previous 15 seconds then they are removed from the list.

/api/getteam 
api that returns a json object of the team members that worked on the project

/api/reset
api that resets the database

/api/zipalertlist
api that displays the zipcodes that are in alert status

/api/alertlist
api that displays if the state is in an alert status 1 for is in alert and 0 for
not.

/api/testcount
api that displays the number of positive covid test

/api/getpatient/{mrn}
gets the patients file by using their mrn number

/api/gethospital/{id}
gets the number of availalbe beds for the hospital using the hospitals id
The program requires the user to use mongodb.
