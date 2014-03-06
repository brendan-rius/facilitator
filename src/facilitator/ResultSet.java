package facilitator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
}
