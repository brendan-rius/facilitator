package facilitator;

import com.importio.api.clientlite.ImportIO;
import com.importio.api.clientlite.MessageCallback;
import com.importio.api.clientlite.data.Progress;
import com.importio.api.clientlite.data.Query;
import com.importio.api.clientlite.data.QueryMessage;
import com.spaceprogram.kittycache.KittyCache;
import facilitator.annotations.Attribute;
import facilitator.annotations.Id;
import org.joda.money.CurrencyUnit;
import org.joda.money.Money;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class IOClient extends ImportIO
{
	protected static IOClient                     instance;
	protected        KittyCache<Query, ResultSet> cache;
	/* Queries will be cached foo one hour */
	protected static final Integer CACHE_TTL = 3600;

	public static IOClient getInstance(String userId, String apiKey)
	{
		if (instance == null)
			{
				instance = new IOClient(userId, apiKey);
			}

		return instance;
	}

	protected IOClient(String userid, String apiKey)
	{
		super(UUID.fromString(userid), apiKey);

		/* We create a cache of 200 queries max with a TTL of one day */
		this.cache = new KittyCache<Query, ResultSet>(2);
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
		if (this.cache.get(query) == null || forceRefresh)
			{
				this.performQuery(query);
			}

		/* We retrieve results from cache (that may have been updated with forceRefresh) */
		ResultSet results = this.cache.get(query);
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
		List<Object> populatedObjects = this.getListFilledWithInstancesOf(toPopulate, results.size());

		/* Note that we look for superclass' fields too */
		List<Field> classFields = this.getAllFieldsOfClassAsList(toPopulate);

		for (Field field : classFields)
			{
				/* If the current field is annotated with @Id, then
				 * we fill all the instances with the id and then
				 * we skip the field since we can't have a field with
				 * both @Attribute and @Id annotations
				 */
				if (this.hasIdAnnotation(field))
					{
						this.setIdToRows(field, results, populatedObjects);
						continue;
					}

				Attribute a;
				if ((a = this.getAttributeAnnotation(field)) != null)
					{
						/* If we are there, it means that the field f is marked with @Attribute */
						String columnName = a.value();

						if (results.hasColumn(columnName))
							{
								/* Is we are there, if means that not only the field is marked with @Attribute,
								 * but also that the result contains the columns that the POJO asks for.
								 */

								/* If so, we look for the type that ImportIO gives us (currency, link, string...) */
								ResultSet.Type type = results.guessColumnType(columnName);
								/* We retrieve what class should we user to fit the data into the field */
								Class compatibilityClass = this.getCompatibleClassBetweenTypeAndField(field, type);
								if (compatibilityClass == null)
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
										this.setField(field, populatedObjects.get(i), this.parseValueAsInstanceOf(row, columnName, compatibilityClass));
										i++;
									}
							}
					}
			}

		return populatedObjects;
	}

	private void setIdToRows(Field f, ResultSet rows, List<Object> objects) throws IllegalAccessException
	{
		Long i = 0l;
		for (Map<String, Object> row : rows)
			{
				this.setField(f, objects.get(i.intValue()), i);
				i++;
			}
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
						IOClient.this.cache.put(query, new ResultSet(results), CACHE_TTL);
					}
				if (progress.isFinished())
					{
						latch.countDown();
					}
			}
		};
		this.query(query, messageCallback);

		latch.await();
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

	protected Boolean hasIdAnnotation(Field f)
	{
		/* Note that we also look for superclass' anotations */
		Annotation annotations[] = f.getAnnotations();
		for (Annotation a : annotations)
			{
				if (a instanceof Id)
					{
						return true;
					}
			}

		return false;
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

		if (targetClass == Date.class)
			{
				/* For date, the column name should contain the timestamp */
				Date toReturn = new Date((Long) row.get(columnName));
				return toReturn;
			}

		if (targetClass == String.class)
			{
				return row.get(columnName);
			}

		return null;
	}

	protected Class getCompatibleClassBetweenTypeAndField(Field f, ResultSet.Type t)
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
}
