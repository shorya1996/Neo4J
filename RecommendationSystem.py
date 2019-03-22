# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 12:34:21 2019

@author: ShoryaSharma
"""
from py2neo import  authenticate, Graph
import pandas as pd
url = "http://localhost:7474/db/data/"

authenticate("localhost:7474", "neo4j", "mypassword")

graph = Graph(url)
query = """CREATE CONSTRAINT ON ( rest:Restaurant ) ASSERT rest.pid IS UNIQUE;"""
data = graph.run(query)
query = """CREATE CONSTRAINT ON ( company:Company ) ASSERT company.name IS UNIQUE;"""
data = graph.run(query)
query = """CREATE CONSTRAINT ON ( usr:User ) ASSERT usr.UserID IS UNIQUE;"""
data = graph.run(query)

query1 = """USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
"file:///C:/Users/ShoryaSharma/Documents/Users_50.csv" 
As line
WITH line

MERGE (company:Company {name: "Ameyasoft"})
MERGE (usr:User {uid: line.UserID})
CREATE (company)-[:USER_PROFILE]->(usr)

CREATE (prsnl:Personal {name: "Personal"})
CREATE (usr)-[:PERSONAL]->(prsnl)

CREATE (intrst:Interest {attr9: line.Interest})
CREATE (prsnl)-[:INTEREST]->(intrst)

CREATE (prsnlt:Persnlty {attr10: line.Personality})
CREATE (prsnl)-[:PERSONALITY]->(prsnlt)

CREATE (trnsp:Transp {attr5: line.Transport})
CREATE (prsnl)-[:TRANSPORT]->(trnsp)

CREATE (marrd:Married {attr6: line.Marital_Status})
CREATE (prsnl)-[:MARITAL_STATUS]->(marrd)


CREATE (brthyr:Byear {attr8: line.Birth_Year})
CREATE (prsnl)-[:BIRTH_YEAR]->(brthyr)


CREATE (relgn:Religion {attr11: line.Religion})
CREATE (prsnl)-[:RELIGION]->(relgn)


CREATE (habit:Habit {name: "Habits"})
CREATE (usr)-[:HABITS]->(habit)

CREATE (smkr:Smoker {attr1: line.Smoker})
CREATE (habit)-[:SMOKER]->(smkr)

CREATE (drnk:Drink {attr2: line.Drink_Level})
CREATE (habit)-[:DRINK_LEVEL]->(drnk)

CREATE (dress:Dress {attr3: line.Dress_Pref})
CREATE (habit)-[:DRESS]->(dress)

CREATE (hijos:Hijos {attr7: line.Hijos})
CREATE (habit)-[:HIJOS]->(hijos)

CREATE (ambnc:Ambnce {attr4: line.Ambience})
CREATE (habit)-[:AMBIENCE]->(ambnc)

CREATE (actv:Activity {attr12: line.Activity})
CREATE (habit)-[:USER_PROFILE]->(actv)

CREATE (budgt:Budget {attr13: line.Budget})
CREATE (habit)-[:BUDGET]->(budgt)

CREATE (m:Cuisine {name: line.UserID})
CREATE (usr)-[:CUISINE]->(m)

;"""
data1 = graph.run(query1)

query2 = """USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
"file:///C:/Users/ShoryaSharma/Documents/UCuisine.csv" 
As line
WITH line

CREATE (f:Food {name: line.Rcuisine})
WITH line, f

MATCH (n:User {uid:line.userID})-[:CUISINE]->(m:Cuisine {name: 'Cuisine'})
MERGE (n)-[:CUISINE]->(m)-[:TYPE]->(f)
;"""
data2 = graph.run(query2)


query3="""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
"file:///C:/Users/ShoryaSharma/Documents/Restaurants.csv" 
As line
WITH line

MERGE (company:Company {name: "Ameyasoft"})
MERGE (rest:Restaurant {pid: toInt(line.PlaceID), name: line.Name})
CREATE (company)-[:RESTAURANT]->(rest)

CREATE (addr:Addrs {street: line.Address, city: line.City, state: line.State, zip: line.Zip, country: line.Country})
CREATE (rest)-[:ADDRESS]->(addr)

CREATE (featrs:Features {alcohol: line.Alcohol, smoking: line.Smoking_Area, dress: line.Dress_Code, price: line.Price, ambience: line.Ambience})
CREATE (rest)-[:FEATURES]->(featrs)

;"""
data3 = graph.run(query3)

query4="""USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
"https://raw.githubusercontent.com/kaisesha/cdrgraph/master/RestCuisine.csv" 
As line
WITH line

MATCH (n:Restaurant {pid: toInt(line.PlaceID)})
MERGE (n)-[:REST_CUISINE]->(cuse:Cusine {name: line.Cuisine});
"""
data4 = graph.run(query4)


query5="""MATCH (c)-[r:USER_PROFILE|RESTAURANT]->(n)-[]->(p)
WHERE n.uid IN['U1001', 'U1002', 'U1003'] or  n.pid IN [132609, 132613, 132630]
RETURN c, n, p LIMIT 20;"""
data5=graph.run(query5)







