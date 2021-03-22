# MapReduce

**Repository of Codes for MapReduce Problems**

1) Text analysis to find out the set of words that are frequently used within a file. For this, you need to write a Map reduce program that identifies all the words whose length > 5 and the frequency of occurrence > 100.

Code : wordsFrequency.java

2) Write a MapReduce job that processes the global weather dataset and returns the records of the country "India". The output should contain 4 different files. Each file would contain weather data of one entire century. 
For example: 4 part files should contain the data in the following pattern:
• File1 : Year 1700-1799 ( All the records of the 18th century will be stored in File1)
• File2 : Year 1800-1899( All the records of the 19th century will be stored in File2)
• File3 : Year 1900-1999( All the records of the 20th century will be stored in File3)
• File4 : Year 2000-Present ( All the records of the 21st century will be stored in File4

Code : splitFiles.java
(Concept of Partitioner)

3) Write a MapReduce program to find the maximum salary of an employee for each department along with the employee Id

Code : maxSalary.java

4) Write a Map Reduce program to find number of employees per location.
![image](https://user-images.githubusercontent.com/19937289/112042303-5e9d5f00-8b6d-11eb-9e6e-8f4e255391ee.png)

Code : employeePerLocation.java
(Used the concept of distributed cache to store the smaller file dept.csv and checking the calculating number of employees per location from emp.csv

5) Inverted index is a database index that maps content, such as words or numbers, to its locations in a table, or in a document or a set of documents. We use inverted index to find out a word's location in a document (or set of documents) as quick as possible.
Given a set of documents that contain employees information. Build an inverted index using MapReduce that returns the First Name and the set of documents/files that contains the respective FirstName.

Sample Output: It should contain the First Name as key and the list of documents that contains this First Name as value.
AARON      employee_info_1.csv      employee_info_2.csv
ABADJR    employee_info_1.csv
ABARCA    employee_info_1.csv

Code : InvertedIndex.java

6) Use counters concept and count the number of time MALCOLM and DUNCAN Say their dialogues in a given text file.

Code : lineCounter.java
