import java.util.ArrayList;
import java.util.List;

public class CreateObjects {

	/**
	 * Avro objects can be created either by invoking a constructor directly or
	 * by using a builder. Unlike constructors, builders will automatically set
	 * any default values specified in the schema. Additionally, builders
	 * validate the data as it set, whereas objects constructed directly will
	 * not cause an error until the object is serialized. However, using
	 * constructors directly generally offers better performance, as builders
	 * create a copy of the datastructure before it is written.
	 */

	/**
	 * ¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡TENER EN CUENTA!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	 *
	 * using a builder requires setting all fields, even if they are null
	 */

	public static List<StringPair> CreateThreeExampleStringPair() {

		// 1: forma de contruir un objeto
		StringPair s1 = new StringPair();
		s1.setLeft("left");
		s1.setRight("right");

		// 2: forma de contruir un objeto
		StringPair s2 = new StringPair("izq", "der");

		// 2: forma de contruir un objeto via Builder
		StringPair s3 = StringPair.newBuilder().setLeft("izquierda").setRight("derecha").build();

		List<StringPair> stringsPairs = new ArrayList<StringPair>();
		stringsPairs.add(s1);
		stringsPairs.add(s2);
		stringsPairs.add(s3);

		return stringsPairs;
	}

	public static void main(String args[]) {
		List<StringPair> list = CreateThreeExampleStringPair();

		System.out.println("s1: " + list.get(0).toString());

		System.out.println("s2: " + list.get(1).toString());

		System.out.println("s3: " + list.get(2).toString());
	}

}
