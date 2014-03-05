package facilitator;

import com.importio.api.clientlite.ImportIO;
import com.importio.api.clientlite.MessageCallback;
import com.importio.api.clientlite.data.Progress;
import com.importio.api.clientlite.data.Query;
import com.importio.api.clientlite.data.QueryMessage;
import facilitator.annotations.Attribute;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class IOClient extends ImportIO
{
	protected HashMap<Query, List<Map<String, Object>>> cachedQueries;

	public IOClient(String userid, String apiKey)
	{
		super(UUID.fromString(userid), apiKey);
		this.cachedQueries = new HashMap<Query, List<Map<String, Object>>>();
	}

	public List populate(Query query, Class toPopulate) throws Exception
	{
		return this.populate(query, toPopulate, false);
	}

	/**
	 * Parse query's result and populate models
	 *
	 * @param query
	 * @param toPopulate
	 * @param forceRefresh
	 * @return
	 * @throws Exception
	 */
	public List populate(Query query, Class toPopulate, Boolean forceRefresh) throws Exception
	{
		/* If the query isn't cached or the user wants very-fresh data, query the server */
		if (!this.cachedQueries.containsKey(query) || forceRefresh)
			{
				this.performQuery(query);
			}

		/* We retrieve results from cache (that may have been updated with forceRefresh) */
		List<Map<String, Object>> results = this.cachedQueries.get(query);
		/* Early-return if no results */
		if (results == null)
			{
				return null;
			}
		if (results.size() <= 0)
			{
				return null;
			}

		/* The list we'll return has the same size than the list of results */
		List<Object> toReturn = this.getListFilledWithInstancesOf(toPopulate, results.size());

		/* Note that we look for superclass' fields too */
		List<Field> fields = this.getAllFieldsOfClassAsList(toPopulate);

		for (Field f : fields)
			{
				Attribute a;
				if ((a = this.getAttributeAnnotation(f)) != null)
					{
						/* If we are there, it means that the field f is marked with @Attribute */
						String columnName = a.value();

						if (results.get(0).containsKey(columnName)) // ugly but working since size > 0 because of early return
							{
								/* Is we are there, if means that not only the fiels is marked with @Attribute,
								 * but also that the result contains the columns that the POJO asks for.
								 */

								/* If so, we look for the type that ImportIO gives us (currency, link, string...) */
								Type type = this.getTypeUsingColumnName(columnName, results);
								/* We retrieve what class should we user to fit the data into the field */
								Class c = this.getCompatibleClassBetweenTypeAndField(f, type);
								if (c == null)
									{
										/* If there's no compatible class, we can fit nothing into the field, so we skip it */
										continue;
									}

								int i = 0;
								for (Map<String, Object> row : results)
									{
										/* Here, we are sure that we can update the field f using an instance
										 * of the class "c". So first off we'll try to create an instance of
										 * "c" depending on the value we got from ImportIO.
										 */
										this.setField(f, toReturn.get(i), this.parseValueAsInstanceOf(row, columnName, c));
										i++;
									}
							}
					}
			}

		return toReturn;
	}

	/**
	 * Creates a list of instances of toPopulate of size "size"
	 *
	 * @param toPopulate
	 * @param size
	 * @return
	 * @throws Exception
	 */
	protected List<Object> getListFilledWithInstancesOf(Class toPopulate, int size) throws Exception
	{
		List<Object> objects = new ArrayList<Object>();

		for (int _i = 0; _i < size; _i++)
			{
				objects.add(toPopulate.newInstance());
			}

		return objects;
	}

	/**
	 * Performs a query and store the result in cache
	 *
	 * @param query
	 */
	private void performQuery(Query query) throws Exception
	{
		final CountDownLatch latch = new CountDownLatch(1);

		this.connect();

		MessageCallback messageCallback = new MessageCallback()
		{
			public void onMessage(Query query, QueryMessage message, Progress progress)
			{
				if (message.getType() == QueryMessage.MessageType.MESSAGE)
					{
						List<Map<String, Object>> results = (List<Map<String, Object>>) ((HashMap<String, Object>) message.getData()).get("results");
						IOClient.this.cachedQueries.put(query, results);
					}
				if (progress.isFinished())
					{
						latch.countDown();
					}
			}
		};
		this.query(query, messageCallback);

		latch.await();
		this.shutdown();
	}

	/**
	 * Returns instance of attribute if it is found, null otherwise
	 *
	 * @param f
	 * @return
	 */
	protected Attribute getAttributeAnnotation(Field f)
	{
		/* Note that we also look for superclass' anotations */
		Annotation annotations[] = f.getAnnotations();
		for (Annotation a : annotations)
			{
				if (a instanceof Attribute)
					{
						return (Attribute) a;
					}
			}

		return null;
	}

	/**
	 * Set the field f of the object toPopulate to value, no matter
	 * if its accessibility.
	 *
	 * @param f
	 * @param toPopulate
	 * @param value
	 * @throws IllegalAccessException
	 */
	protected void setField(Field f, Object toPopulate, Object value) throws IllegalAccessException
	{
		Boolean wasAccessible = f.isAccessible();
		f.setAccessible(true);

		f.set(toPopulate, value);
		/*
		 * Now that the value has been changed, or the program
		 * failed, we reset the accessibility of the field
		 */
		f.setAccessible(wasAccessible);
	}

	/**
	 * Returns a list of all the fields that the class "c" contains
	 *
	 * @param c
	 * @return
	 */
	protected List<Field> getAllFieldsOfClassAsList(Class c)
	{
		List<Field> fields = new ArrayList<Field>();
		Field f1[] = c.getFields();
		Field f2[] = c.getDeclaredFields();
		fields.addAll(Arrays.asList(f1));
		fields.addAll(Arrays.asList(f2));

		return fields;
	}

	/**
	 * Return the type that should be use to parse the value depending on the
	 * column name.
	 * For example, for the columnName "XXX", if it exists "XXX/_currency", then
	 * we should parse this value as being money.
	 *
	 * @param columnName
	 * @param results
	 * @return
	 */
	protected Type getTypeUsingColumnName(String columnName, List<Map<String, Object>> results)
	{
		if (results.get(0).containsKey(columnName + "/_currency"))
			{
				return Type.MONEY;
			}
		else
			{
				return Type.STRING;
			}
	}

	/**
	 * Create a new instance of "targetClass" and initialize it withs values from ImportIO's row
	 * knowing that the columnName is "columnName".
	 * For example, if columnName is "price" and targetClass is Money, then we'll create a new
	 * instance of Money and parse the column "price/_currency" to get money's currency.
	 *
	 * @param row
	 * @param columnName
	 * @param targetClass
	 * @return
	 */
	protected Object parseValueAsInstanceOf(Map<String, Object> row, String columnName, Class targetClass)
	{
		if (targetClass == Money.class)
			{
				/* For money, XXX represent the money like "20.3"
				 * while XXX/_currency represent the currency unit.
				 * We cannot use Money.parse() because of
				 * https://github.com/JodaOrg/joda-money/issues/35
				 */
				String currencyCode = (String) row.get(columnName + "/_currency");
				CurrencyUnit unit = CurrencyUnit.getInstance(currencyCode);
				BigDecimal value = BigDecimal.valueOf((Double) row.get(columnName));
				Money toReturn = Money.of(unit, value);
				return toReturn;
			}

		if (targetClass == String.class)
			{
				return row.get(columnName);
			}

		return null;
	}

	protected Class getCompatibleClassBetweenTypeAndField(Field f, Type t)
	{
		if (!t.isCompatibleWith(f.getType()) && f.getType() == String.class)
			{
				/* If we didn't found any compatibility between the value returned and
				 * the field, but we know that field is of String type, then we can
				 * still put the raw value in the field, since all returned values
				 * are strings
				 */
				return String.class;
			}
		else if (t.isCompatibleWith(f.getType()))
			{
				/* If the type is compatible with the field, it means that the
				 * field still has the right class. Just return it.
				 */
				return f.getType();
			}
		else
			{
				/* The field is not compatible at all, return null */
				return null;
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
				});

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
