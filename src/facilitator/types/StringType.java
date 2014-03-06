package facilitator.types;

import java.util.Map;

/**
 * User: brendan
 * Date: 06/03/14
 * Time: 19:49
 */

public class StringType extends Type
{
	public StringType()
	{
		this.compatibleClasses.add(String.class);
	}

	@Override
	public Object getValueFromRowsAs(Map<String, Object> row, String columnName, Class c)
	{
		return row.get(columnName).toString();
	}

	@Override
	public Boolean isColumnOfType(Map<String, Object> exampleRow, String columnName)
	{
		/* Anything can be converted to string */
		return true;
	}
}
