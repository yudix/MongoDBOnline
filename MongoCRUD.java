package harav.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoCRUD {

	public static void main(String[] args) {
		new MongoCRUD();
	}

	public MongoCRUD() {
		init();
	}

	public void init() {
		try {
			DBCollection collection = gettingCollection();
			// Person without id
			Person person = new Person("Yehuda", "Ginsburg");

			// Create
			// inserting person into mongo collection

			BasicDBObject dbObjectFromPerson = insertToCollection(person,
					collection);
			System.out.println("inserting Person to Collection" + "\n");
			System.out.println("Person as JSON Object\n" + dbObjectFromPerson
					+ "\n");

			// Read
			// getting a person with generated id from mongo
			// if you know there is just one object like this
			// (mongo allowed duplicated documents(rows)
			Person personWithID = gettingPersonFromDBObject(dbObjectFromPerson,
					collection);
			System.out.println("Persons as Java Objects\n" + personWithID
					+ "\n");

			// //Read from DBObject
			List<Person> personsWithID = gettingPersonsFromDBObject(
					dbObjectFromPerson, collection);

			String messageOne = "Read from DBObject";
			printCollection(messageOne, personsWithID);

			// Read with WHERE
			List<Person> personWhereName = gettingPersonWhere(collection,
					"name", "Yehuda");

			String messageTwo = "Read with WHERE";
			printCollection(messageTwo, personWhereName);

			// Read with WHERE - AND
			List<Person> personWhereNameAndPassword = gettingPersonWhereAnd(
					collection, "name", "Yehuda", "lastName", "Ginsburg");

			String messageThree = "Read with WHERE - AND";
			printCollection(messageThree, personWhereNameAndPassword);

			// Update lastName to be Gin instead of Ginsburg
			BasicDBObject change = setChange("lastName", "Gin");// ("_id", new
																// ObjectId(new
																// Date()));
			BasicDBObject where = setWhere("name", "Yehuda");
			collection.update(where, change);

			String messageFour = "Update the Person last name";
			List<Person> allPersons = allPersons(collection);
			printCollection(messageFour, allPersons);

			// Delete all Documents
			String messageFive = "Delete all Documents";
			removeAllDocuments(collection);

			allPersons = allPersons(collection);
			printCollection(messageFive, allPersons);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public DBCollection gettingCollection() throws UnknownHostException {
		String server = "localhost";
		int port = 27017;
		String dbName = "myFirstDB";
		String collectionName = "myCollection";

		MongoClient client = new MongoClient(server, port);
		System.out.println("Getting Client \"" +client.getClass().getSimpleName()+ "\"\n");

		System.out.println("Getting \"" + dbName + "\" DataBase" + "\n");
		DB myDB = client.getDB(dbName);

		System.out.println("Getting \"" + collectionName + "\" Collection" + "\n");
		DBCollection collections = myDB.getCollection(collectionName);

		return collections;
	}

	public BasicDBObject insertToCollection(Person person,
			DBCollection collection) {

		String name = person.getName();
		String lastName = person.getLastName();

		BasicDBObject document = new BasicDBObject();
		document.put("name", name);
		document.put("lastName", lastName);

		collection.insert(document);

		return document;
	}

	public List<Person> gettingPersonWhereAnd(DBCollection collection,
			String keyOne, Object valueOne, String keyTwo, Object valueTwo) {
		List<Person> persons = new ArrayList<Person>();

		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		BasicDBObject andQuery = new BasicDBObject();

		obj.add(new BasicDBObject(keyOne, valueOne));
		obj.add(new BasicDBObject(keyTwo, valueTwo));

		andQuery.put("$and", obj);

		DBCursor cursor = collection.find(andQuery);
		while (cursor.hasNext()) {
			DBObject dbObject = cursor.next();

			Object id = dbObject.get("_id");
			String name = dbObject.get("name") + "";
			String lastName = dbObject.get("lastName") + "";

			Person person = new Person(name, lastName);
			person.setId(id);

			persons.add(person);

		}
		return persons;
	}

	public List<Person> gettingPersonWhere(DBCollection collection, String key,
			Object value) {
		List<Person> persons = new ArrayList<Person>();
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put(key, value);
		DBCursor cursor = collection.find(whereQuery);
		while (cursor.hasNext()) {
			DBObject dbObject = cursor.next();

			Object id = dbObject.get("_id");
			String name = dbObject.get("name") + "";
			String lastName = dbObject.get("lastName") + "";

			Person person = new Person(name, lastName);
			person.setId(id);
			persons.add(person);
		}
		return persons;
	}

	public Person gettingPersonFromDBObject(BasicDBObject basicDbObject,
			DBCollection collection) {
		DBObject dbObject = collection.findOne(basicDbObject);

		Object id = dbObject.get("_id");
		String name = dbObject.get("name") + "";
		String lastName = dbObject.get("lastName") + "";

		Person person = new Person(name, lastName);
		person.setId(id);

		return person;
	}

	public List<Person> gettingPersonsFromDBObject(BasicDBObject dbObject,
			DBCollection collection) {
		List<Person> persons = new ArrayList<Person>();
		DBCursor cursor = collection.find(dbObject);
		while (cursor.hasNext()) {
			dbObject = (BasicDBObject) cursor.next();
			Object id = dbObject.get("_id");
			String name = dbObject.get("name") + "";
			String lastName = dbObject.get("lastName") + "";
			Person person = new Person(name, lastName);
			person.setId(id);
			persons.add(person);
		}
		return persons;
	}

	public static void removeAllDocuments(DBCollection collection) {
		collection.remove(new BasicDBObject());
	}

	public List<Person> allPersons(DBCollection collection) {
		List<Person> persons = new ArrayList<Person>();
		DBCursor cursor = collection.find();
		while (cursor.hasNext()) {
			DBObject dbObject = cursor.next();
			Object id = dbObject.get("_id");
			String name = dbObject.get("name") + "";
			String lastName = dbObject.get("lastName") + "";
			Person person = new Person(name, lastName);
			person.setId(id);
			persons.add(person);
		}
		return persons;
	}

	public BasicDBObject setChange(String changeKey, Object changeValue) {
		BasicDBObject change = new BasicDBObject();
		BasicDBObject setWithChange = new BasicDBObject();

		change.append(changeKey, changeValue);
		setWithChange.append("$set", change);

		return setWithChange;
	}

	public BasicDBObject setWhere(String whereKey, Object whereValue) {
		BasicDBObject searchQueryWhere = new BasicDBObject();

		searchQueryWhere.append(whereKey, whereValue);

		return searchQueryWhere;
	}

	public static void printCollection(String message, List list) {
		System.out.println(message);
		for (Object o : list)
			System.out.println(o);
		System.out.println();
	}

}
