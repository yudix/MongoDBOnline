package harav.mongodb;

public class Person {
	private Object id;
	private String name;
	private String lastName;

	public Person(String name, String lastName) {
		super();
		this.name = name;
		this.lastName = lastName;
	}

	public Object getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getLastName() {
		return lastName;
	}

	public void setId(Object id) {
		this.id = id;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	@Override
	public String toString() {
		return "Person [ id = " + id + " , name = " + name + " , lastName = "
				+ lastName + " ]";
	}
}
