package facilitator;

import facilitator.types.DateType;
import facilitator.types.MoneyType;
import facilitator.types.StringType;
import facilitator.types.Type;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * User: brendan
 * Date: 06/03/14
 * Time: 19:14
 */

public class TypeManager
{
	/* We use a HashSet because we can't have to identical types
	 * and we need a constant-time add operation
	 */
	protected Set<Type> types = new HashSet<Type>();
	protected Type defaultType;

	/**
	 * Returns the type of the column using an example
	 * row.
	 *
	 * @param exampleRow
	 * @param columnName
	 * @return
	 */
	public Type getTypeOfColumn(Map<String, Object> exampleRow, String columnName)
	{
		for (Type t : types)
			{
				if (t.isColumnOfType(exampleRow, columnName))
					{
						return t;
					}
			}

		return defaultType;
	}

	public void registerType(Type t)
	{
		this.types.add(t);
	}

	public void setDefaultType(Type t)
	{
		this.defaultType = t;
	}

	/**
	 * Returns an instance of a type manager containing
	 * the classic ImportIO's types.
	 *
	 * @return
	 */
	public static TypeManager getClassicTypeManager()
	{
		TypeManager typeManager = new TypeManager();
		typeManager.registerType(new DateType());
		typeManager.registerType(new MoneyType());
		typeManager.setDefaultType(new StringType());

		return typeManager;
	}
}
