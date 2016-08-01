package nl.inl.blacklab.server.jobs;

import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.exceptions.BlsException;
import nl.inl.blacklab.server.search.SearchManager;

/**
 * Represents a docs search and sort operation.
 */
public class JobDocsSorted extends JobWithDocs {

	public static class JobDescDocsSorted extends JobDescription {

		DocSortSettings sortSettings;

		public JobDescDocsSorted(JobDescription hitsToSort, DocSortSettings sortSettings) {
			super(JobDocsSorted.class, hitsToSort);
			this.sortSettings = sortSettings;
		}

		@Override
		public DocSortSettings getDocSortSettings() {
			return sortSettings;
		}

		@Override
		public String uniqueIdentifier() {
			return super.uniqueIdentifier() + "[" + sortSettings + "]";
		}

		@Override
		public DataObjectMapElement toDataObject() {
			DataObjectMapElement o = super.toDataObject();
			o.put("sortSettings", sortSettings.toString());
			return o;
		}

	}

	private DocResults sourceResults;

	public JobDocsSorted(SearchManager searchMan, User user, JobDescription par) throws BlsException {
		super(searchMan, user, par);
	}

	@Override
	public void performSearch() throws BlsException {
		sourceResults = ((JobWithDocs)inputJob).getDocResults();
		setPriorityInternal();
		// Now, sort the docs.
		DocSortSettings docSortSett = jobDesc.getDocSortSettings();
		if (docSortSett.sortBy() != null) {
			// Be lenient of clients passing wrong sortBy values; ignore bad sort requests
			sourceResults.sort(docSortSett.sortBy(), docSortSett.reverse()); // TODO: add .sortedBy() same as in Hits
		}
		docResults = sourceResults; // client can use results
	}

	@Override
	public DataObjectMapElement toDataObject(boolean debugInfo) throws BlsException {
		DataObjectMapElement d = super.toDataObject(debugInfo);
		d.put("numberOfDocResults", docResults == null ? -1 : docResults.size());
		return d;
	}

	@Override
	protected DocResults getObjectToPrioritize() {
		return sourceResults;
	}

}
