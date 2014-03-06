package facilitator.types;

import org.joda.money.CurrencyUnit;
import org.joda.money.Money;

import java.math.BigDecimal;
import java.util.Map;

/**
 * User: brendan
 * Date: 06/03/14
 * Time: 18:21
 */

/**
 * When a column XXX represents money, ImportIO sends
 * us back a some additional rows:
 * - XXX: represent the value like "20.3"
 * - XXX/_currency: represent the code of the currency like "USD"
 * - XXX/_source: represent what ImportIO seen, like "$20.3"
 */
public class MoneyType extends Type
{
	public MoneyType()
	{
		/* money type can be converted to Money, to string or to double */
		this.compatibleClasses.add(Money.class);
		this.compatibleClasses.add(String.class);
		this.compatibleClasses.add(Double.class);
	}

	@Override
	public Object getValueFromRowsAs(Map<String, Object> row, String columnName, Class c)
	{
		if (c == Money.class)
			{
				return this.getAsMoney(row, columnName);
			}
		else if (c == String.class)
			{
				return this.getAsString(row, columnName);
			}
		else if (c == Double.class)
			{
				return this.getAsDouble(row, columnName);
			}
		else
			{
				return null;
			}
	}

	@Override
	public Boolean isColumnOfType(Map<String, Object> exampleRow, String columnName)
	{
		/* See class documentation of this class */
		if (exampleRow.containsKey(columnName)
			&& exampleRow.containsKey(columnName + "/_source")
			&& exampleRow.containsKey(columnName + "/_currency"))
			{
				return true;
			}
		else
			{
				return false;
			}
	}

	protected Money getAsMoney(Map<String, Object> row, String columnName)
	{
		/* We cannot use Money.parse() on XXX/_source because of
		 * https://github.com/JodaOrg/joda-money/issues/35
		 */
		String currencyCode = (String) row.get(columnName + "/_currency");
		CurrencyUnit unit = CurrencyUnit.getInstance(currencyCode);
		BigDecimal value = BigDecimal.valueOf((Double) row.get(columnName));
		Money toReturn = Money.of(unit, value);

		return toReturn;
	}

	protected String getAsString(Map<String, Object> row, String columnName)
	{
		return (String) row.get(columnName + "/_source");
	}

	protected Double getAsDouble(Map<String, Object> row, String columnName)
	{
		return (Double) row.get(columnName);
	}
}
