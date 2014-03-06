package facilitator.types;

import java.util.Date;
import java.util.Map;

/**
 * User: brendan
 * Date: 06/03/14
 * Time: 18:22
 */

/**
 * When a column XXX represents a date, ImportIO sends
 * us back a some additional rows:
 * - XXX: represents the timestamp
 * - XXX/_source: represent the litteral date that ImportIO found
 * - XXX/_utc: represent the clean litteral date
 */
public class DateType extends Type
{
	public DateType()
	{
		this.compatibleClasses.add(Date.class);
		this.compatibleClasses.add(Long.class);
		this.compatibleClasses.add(String.class);
	}

	@Override
	public Object getValueFromRowsAs(Map<String, Object> row, String columnName, Class c)
	{
		if (c == Date.class)
			{
				return this.getAsDate(row, columnName);
			}
		else if (c == Long.class)
			{
				return this.getAsLong(row, columnName);
			}
		else if (c == String.class)
			{
				return this.getAsString(row, columnName);
			}
		else
			{
				return null;
			}
	}

	@Override
	public Boolean isColumnOfType(Map<String, Object> exampleRow, String columnName)
	{
		if (exampleRow.containsKey(columnName)
			&& exampleRow.containsKey(columnName + "/_source")
			&& exampleRow.containsKey(columnName + "/_utc"))
			{
				return true;
			}
		else
			{
				return false;
			}
	}

	protected Long getAsLong(Map<String, Object> row, String columnName)
	{
		return (Long) row.get(columnName);
	}

	protected Date getAsDate(Map<String, Object> row, String columnName)
	{
		return new Date((Long) row.get(columnName));
	}

	protected String getAsString(Map<String, Object> row, String columnName)
	{
		return (String) row.get(columnName + "/_utc");
	}
}
