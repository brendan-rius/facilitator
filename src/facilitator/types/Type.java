package facilitator.types;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: brendan
 * Date: 06/03/14
 * Time: 15:52
 */

/* TODO: use static se that the HashMap in TypeManager
 * gets some sense
 */
public abstract class Type
{
	protected List<Class> compatibleClasses = new ArrayList<Class>();

	/**
	 * Returns a list of classes that the type can be
	 * converted in
	 *
	 * @return
	 */
	public List<Class> getCompatibleClasses()
	{
		return this.compatibleClasses;
	}

	/**
	 * Returns an instance of the value from the "row" at the column
	 * "columnName" converted (if possible) to class c.
	 * If conversion in impossible, returns null.
	 *
	 * @param row
	 * @param columnName
	 * @param c
	 * @return
	 */
	public abstract Object getValueFromRowsAs(Map<String, Object> row, String columnName, Class c);

	/**
	 * Returns true is the column is from this type, false otherwise.
	 *
	 * @param exampleRow
	 * @param columnName
	 * @return
	 */
	public abstract Boolean isColumnOfType(Map<String, Object> exampleRow, String columnName);

	/**
	 * Returns true is the field has a compatibility with the class
	 *
	 * @param f
	 * @return
	 */
	public Boolean isCompatibleWithfield(Field f)
	{
		if (this.compatibleClasses.contains(f.getType()))
			{
				return true;
			}
		else
			{
				return false;
			}
	}
}
