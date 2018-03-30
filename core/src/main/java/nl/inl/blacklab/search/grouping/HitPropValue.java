package nl.inl.blacklab.search.grouping;

import java.text.Collator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import nl.inl.blacklab.search.Hits;
import nl.inl.util.StringUtil;

/**
 * A concrete value of a HitProperty of a Hit
 *
 * Implements <code>Comparable&lt;Object&gt;</code> as opposed to something more specific
 * for performance reasons (preventing lots of runtime type checking during
 * sorting of large results sets)
 */
public abstract class HitPropValue implements Comparable<Object> {
	protected static final Logger logger = LogManager.getLogger(HitPropValue.class);

	/**
	 * Collator to use for string comparison while sorting/grouping
	 */
	static Collator collator = StringUtil.getDefaultCollator();

	@Override
	public abstract int compareTo(Object o);

	@Override
	public abstract int hashCode();

	@Override
	public boolean equals(Object obj) {
		return compareTo(obj) == 0;
	}

	/**
	 * Convert the String representation of a HitPropValue back into the HitPropValue
	 * @param hits hits object (for context word related HitPropValues)
	 * @param serialized the serialized object
	 * @return the HitPropValue object, or null if it could not be deserialized
	 */
	public static HitPropValue deserialize(Hits hits, String serialized) {

		if (PropValSerializeUtil.isMultiple(serialized))
			return HitPropValueMultiple.deserialize(hits, serialized);

		String[] parts = PropValSerializeUtil.splitPartFirstRest(serialized);
		String type = parts[0].toLowerCase();
		String info = parts.length > 1 ? parts[1] : "";

		switch (type) {
		case "cwo": return HitPropValueContextWord.deserialize(hits, info);
		case "cws": return HitPropValueContextWords.deserialize(hits, info);
		case "dec": return HitPropValueDecade.deserialize(info);
		case "int": return HitPropValueInt.deserialize(info);
		case "str": return HitPropValueString.deserialize(info);
//		case "mul": return HitPropValueMultiple.deserialize(searcher, info);
		default: logger.debug("Unknown HitPropValue '" + type + "'"); return null;
		}
	}

	/**
	 * Convert the object to a String representation, for use in e.g. URLs.
	 * @return the serialized object
	 */
	public abstract String serialize();

	@Override
	public abstract String toString();

	public abstract List<String> getPropValues();
}
