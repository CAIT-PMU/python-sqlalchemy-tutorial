import sqlalchemy as db
import pandas as pd

##Example of connection
#engine = db.create_engine('dialect+driver://user:pass@host:port/db')

# use a sqlite db in ram
#engine = create_engine('sqlite://')
####engine = create_engine('sqlite:///test-db')

engine = db.create_engine('sqlite:///census.sqlite')
connection = engine.connect()
metadata = db.MetaData()
census = db.Table('census', metadata, autoload=True, autoload_with=engine)

# Print the column names
print(census.columns.keys())

# Print full table metadata
print(repr(metadata.tables['census']))

#Equivalent to 'SELECT * FROM census'
query = db.select([census])
ResultProxy = connection.execute(query)
ResultSet = ResultProxy.fetchall()
ResultSet[:3]

##Make a dataframe
df = pd.DataFrame(ResultSet)
df.columns = ResultSet[0].keys()


## Filtering
#"Where"
query = db.select([census]).where(census.columns.sex == 'F')
result = connection.execute(query).scalar()
print(result)
# "in"
query = db.select([census.columns.state, census.columns.sex]).where(census.columns.state.in_(['Texas', 'New York']))
result = connection.execute(query).scalar()
print(result)
# "and,or,not"
query = db.select([census]).where(db.and_(census.columns.state == 'California', census.columns.sex != 'M'))
result = connection.execute(query).scalar()
print(result)
# "orderby"
query = db.select([census]).order_by(db.desc(census.columns.state), census.columns.pop2000)
result = connection.execute(query).scalar()
print(result)
# "functions"
query = db.select([db.func.sum(census.columns.pop2008)])
result = connection.execute(query).scalar()
print(result)

# "group-by"
query = db.select([db.func.sum(census.columns.pop2008).label('pop2008'), census.columns.sex]).group_by(census.columns.sex)
result = connection.execute(query).scalar()
print(result)

# "distinct"
query = db.select([census.columns.state.distinct()])
result = connection.execute(query).scalar()
print(result)

# "case and cast"
female_pop = db.func.sum(db.case([(census.columns.sex == 'F', census.columns.pop2000)],else_=0))
total_pop = db.cast(db.func.sum(census.columns.pop2000), db.Float)
query = db.select([female_pop/total_pop * 100])
result = connection.execute(query).scalar()
print(result)

# "Joins"
census = db.Table('census', metadata, autoload=True, autoload_with=engine)
state_fact = db.Table('state_fact', metadata, autoload=True, autoload_with=engine)

query = db.select([census.columns.pop2008, state_fact.columns.abbreviation])
result = connection.execute(query).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(5)

#"Manual version of join above"

query = db.select([census, state_fact])
query = query.select_from(census.join(state_fact, census.columns.state == state_fact.columns.name))
results = connection.execute(query).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(5)

# "Creating and inserting data into tables"

#Inserting record one by one
query = db.insert(emp).values(Id=1, name='naveen', salary=60000.00, active=True)
ResultProxy = connection.execute(query)


#Inserting many records at ones
query = db.insert(emp)
values_list = [{'Id':'2', 'name':'ram', 'salary':80000, 'active':False},
               {'Id':'3', 'name':'ramesh', 'salary':70000, 'active':True}]
ResultProxy = connection.execute(query,values_list)


results = connection.execute(db.select([emp])).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(4)

#Updating data in Databases
emp = db.Table('emp', metadata, autoload=True, autoload_with=engine)

results = connection.execute(db.select([emp])).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(4)

# Build a statement to update the salary to 100000
query = db.update(emp).values(salary = 100000)
query = query.where(emp.columns.Id == 1)
results = connection.execute(query)
print (results)


results = connection.execute(db.select([emp])).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(4)

# "Delete Table"
engine = db.create_engine('sqlite:///test.sqlite')
metadata = db.MetaData()
connection = engine.connect()
emp = db.Table('emp', metadata, autoload=True, autoload_with=engine)


results = connection.execute(db.select([emp])).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(4)

query = db.delete(emp)
query = query.where(emp.columns.salary < 100000)
results = connection.execute(query)

results = connection.execute(db.select([emp])).fetchall()
df = pd.DataFrame(results)
df.columns = results[0].keys()
df.head(4)

# "Dropping a Table"
# table_name.drop(engine) #drops a single table
# metadata.drop_all(engine) #drops all the tables in the database
