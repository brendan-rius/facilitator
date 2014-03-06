package facilitator;

import org.joda.money.Money;

import java.util.*;

/**
 * User: brendan
 * Date: 06/03/14
 * Time: 14:31
 */

public class ResultSet implements Iterable<Map<String, Object>>
{
	protected List<Map<String, Object>> rows;

	public ResultSet(List<Map<String, Object>> rows)
	{
		this.rows = rows;
	}

	public Integer size()
	{
		return rows.size();
	}

	@Override
	public Iterator<Map<String, Object>> iterator()
	{
		return rows.iterator();
	}

	/**
	 * Returns true if the set of results has a column named
	 * columnName. False otherwise.
	 * Note: false will be returned too if there are no results
	 *
	 * @param columnName
	 * @return
	 */
	public Boolean hasColumn(String columnName)
	{
		if (this.size() <= 0)
			{
				return false;
			}
		else
			{
				if (this.rows.get(0).containsKey(columnName))
					{
						return true;
					}
				else
					{
						return false;
					}
			}
	}

	/**
	 * Return the type that should be use to parse the value depending on the
	 * column name.
	 * For example, for the columnName "XXX", if it exists "XXX/_currency", then
	 * we should parse this value as being money.
	 *
	 * @param columnName
	 * @return
	 */
	public Type guessColumnType(String columnName)
	{
		/* Money type contains XXX/_currency */
		if (this.hasColumn(columnName + "/_currency"))
			{
				return Type.MONEY;
			}
		/* Dates contains XXX/_utc */
		else if (this.hasColumn(columnName + "/_utc"))
			{
				return Type.DATE;
			}
		else
			{
				return Type.STRING;
			}
	}

	/**
	 * Those types represent the different type that columns can be
	 */
	public static enum Type
	{
		STRING(String.class),
		MONEY
			(new Class[]
				{
					Money.class,
					String.class
				}),
		DATE(Date.class);

		private List<Class> compatibilities = new ArrayList<Class>();

		Type(Class compatibilities[])
		{
			this.compatibilities.addAll(Arrays.asList(compatibilities));
		}

		Type(Class compatibility)
		{
			this.compatibilities.add(compatibility);
		}

		public Boolean isCompatibleWith(Class c)
		{
			return this.compatibilities.contains(c);
		}
	}
}
