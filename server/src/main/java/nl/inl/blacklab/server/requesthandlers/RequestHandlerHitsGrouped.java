package nl.inl.blacklab.server.requesthandlers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.perdocument.DocGroup;
import nl.inl.blacklab.perdocument.DocGroupPropertySize;
import nl.inl.blacklab.perdocument.DocGroups;
import nl.inl.blacklab.perdocument.DocProperty;
import nl.inl.blacklab.perdocument.DocPropertyComplexFieldLength;
import nl.inl.blacklab.perdocument.DocPropertyMultiple;
import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.ResultsWindow;
import nl.inl.blacklab.search.grouping.HitGroup;
import nl.inl.blacklab.search.grouping.HitGroups;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.datastream.DataStream;
import nl.inl.blacklab.server.exceptions.BlsException;
import nl.inl.blacklab.server.jobs.DocGroupSettings;
import nl.inl.blacklab.server.jobs.DocGroupSortSettings;
import nl.inl.blacklab.server.jobs.JobDescription;
import nl.inl.blacklab.server.jobs.JobDocs;
import nl.inl.blacklab.server.jobs.JobDocsGrouped;
import nl.inl.blacklab.server.jobs.JobDocsGrouped.JobDescDocsGrouped;
import nl.inl.blacklab.server.jobs.JobHitsGrouped;
import nl.inl.blacklab.server.jobs.JobWithDocs;
import nl.inl.blacklab.server.jobs.User;
import nl.inl.blacklab.server.jobs.WindowSettings;

/**
 * Request handler for grouped hit results.
 */
public class RequestHandlerHitsGrouped extends RequestHandler {

	public RequestHandlerHitsGrouped(BlackLabServer servlet, HttpServletRequest request, User user, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, user, indexName, urlResource, urlPathPart);
	}

	// perform some binding between the docgroups and hitgroups
	// register the total tokens in the docgroup with every hitgroup that contains all the same group identities (and more) as the document group
	private void enrich(HitGroups hitGroups, DocGroups docGroups) throws BlsException {
		// the identity of the group should be the same - but without the hitgroup values taken into account


		String tokenMainField = getSearcher().getIndexStructure().getMainContentsField().getName();
		DocProperty propTokens = new DocPropertyComplexFieldLength(tokenMainField);

		/*
		 * Essentially what we have here is:
		 *
		 * docGroups: these are filtered by the metadata filter, so they contain all documents matching the filter, with or without hits
		 * these are grouped on the grouping properties applicable to documents.
		 *
		 * hitGroups: these are grouped on all properties, also those only applicable to hits (context, token properties, etc..)
		 * so realistically currently there are potentially many hitGroups for each DocGroup
		 * Jan's proposal is to forbid grouping on hit properties when viewing frequencies,
		 *  in other words only showing frequencies when not grouping on hit properties
		 *
		 * We might also be able to adapt the HitGroup object to take into account all input docGroups, not just containing the actual hits.
		 * we'd probably need 2 getter functions: getInputDocumentsWithHits, getAllInputDocuments (or something)
		 * 
		 * On top of that we need to calculate 2 frequencies:
		 *  - relative to all documents matching the filter (sum of tokens in all docgroups)
		 *  - relative to documents in the specific group (sum of all tokens in only the matching docgroup for the hitgroup)
		 */
		for (DocGroup dg : docGroups) {
			List<String> groupPropertyValues = dg.getIdentity().getPropValues();

			StreamSupport.stream(hitGroups.spliterator(), false)
			.filter(hg -> hg.getIdentity().getPropValues().containsAll(groupPropertyValues))
			.forEach(hg -> logger.info("matched hitgroup {} ({} hits) with docgroup {} containing {} documents ({} hits)",
					hg.getIdentity(),
					hg.size(),
					dg.getIdentity(),
					dg.size(),
					dg.getResults().intSum(propTokens))); // cache?

			//.findFirst().ifPresent(consumer);
		}
	}

	@Override
	public int handle(DataStream ds) throws BlsException {
		// Get the window we're interested in
		JobHitsGrouped search = (JobHitsGrouped) searchMan.search(user, searchParam.hitsGrouped(), isBlockingOperation());
		try {
			// If search is not done yet, indicate this to the user
			if (!search.finished()) {
				return Response.busy(ds, servlet);
			}

			// Search is done; construct the results object
			final HitGroups groups = search.getGroups();

			ds.startMap();
			ds.startEntry("summary").startMap();
			Hits hits = search.getHits();
			WindowSettings windowSettings = searchParam.getWindowSettings();
			final int first = windowSettings.first() < 0 ? 0 : windowSettings.first();
			final int requestedWindowSize = windowSettings.size() < 0 || windowSettings.size() > searchMan.config().maxPageSize() ? searchMan.config().defaultPageSize() : windowSettings.size();
			int totalResults = groups.numberOfGroups();
			final int actualWindowSize = first + requestedWindowSize > totalResults ? totalResults - first : requestedWindowSize;
			ResultsWindow ourWindow = new ResultsWindowImpl(totalResults, first, requestedWindowSize, actualWindowSize);
			addSummaryCommonFields(ds, searchParam, search.userWaitTime(), 0, hits, hits, false, (DocResults)null, groups, ourWindow);
			ds.endMap().endEntry();



			boolean includeTokenCount = searchParam.getBoolean("includetokencount");
			if (includeTokenCount) {
//				SearchParameters originalSearchParam = searchParam;
//				searchParam = servlet.getSearchParameters(true, request, indexName);

				// so what we need is ALL documents matching the filters on documents, grouped the same way as the hits, minus any token specific groupings (lemma, pos, word etc.)
				// if the only grouping is on token properties, there's only one document group; the whole set of documents

				// first construct the all document query with document filters

				// only difference with regular searchParam.docsGrouped() is that the hitsToGroup input parameters on the root JobDescDocs is null,
				// thus it returns all documents instead of only those with hits matching any pattern
//				if (searchParam.docGroupSettings() != null) // grouping on documents included with

				DocGroups docGroups = null;
				DocResults docs = null;

				// Manually parse group properties, then remove all nulls, which are failed parsings of group properties that apply to hits instead of docs
				DocProperty docGrouping = DocProperty.deserialize(searchParam.getString("group"));
				if (docGrouping instanceof DocPropertyMultiple) {
					List<DocProperty> subProps = StreamSupport.stream(((DocPropertyMultiple) docGrouping).spliterator(), false).filter(prop -> prop != null).collect(Collectors.toList());
					docGrouping = subProps.size() == 1 ? subProps.get(0) : new DocPropertyMultiple(subProps.toArray(new DocProperty[subProps.size()]));
				}


				JobDescription descAllDocsMatchingFilter = new JobDocs.JobDescDocs(searchParam, null, searchParam.getSearchSettings(), searchParam.getFilterQuery(), searchParam.getIndexName());
				// we have some form on grouping on document, now run the grouped document search and get the results
				if (docGrouping != null) {
					JobDescDocsGrouped descAllDocsMatchingFilterGrouped = new JobDocsGrouped.JobDescDocsGrouped(searchParam, descAllDocsMatchingFilter, searchParam.getSearchSettings(), new DocGroupSettings(docGrouping), new DocGroupSortSettings(new DocGroupPropertySize(), false));
					JobDocsGrouped job = (JobDocsGrouped) searchMan.search(user, descAllDocsMatchingFilterGrouped, true);
					job.decrRef();

					docGroups = job.getGroups();
					docs = docGroups.getOriginalDocResults(); // should just be all docs (from the input job)?
				} else {
					JobWithDocs job = (JobWithDocs) searchMan.search(user, descAllDocsMatchingFilter, true);
					job.decrRef();
					docs = job.getDocResults();
				}

				// now for the fun part, linking the doc group with the hit group...
				// how do we even do this...

				// we need to extract the doc group identity from all hitsgroups, and then find the matching docgroup for that
				// and we need to do that for the instance of the group without filtering by docs with hits in them, and docs with hits in them
				logger.info("printing all hitgroups");
				for (HitGroup hg : groups) {
					// how do we know which part of the HitPropValue for this group applies to the hits, and which part applies to the docs
					// this may only be known deeper in the structure of the searcher?
					logger.info("\t" + hg.getIdentity());
				}

				logger.info("printing all docgroups");
				if (docGroups != null) {
					for (DocGroup dg : docGroups) {
						logger.info("\t" + dg.getIdentity());
					}
				} else {
					logger.info("\tno docGroups, (interpret as 1 group with a size of {} documents", docs.countSoFarDocsCounted());
				}


				if (docGroups != null)
					enrich(groups, docGroups);
			}


			// The list of groups found
			ds.startEntry("hitGroups").startList();
			int i = 0;
			for (HitGroup group: groups) {
				if (i >= first && i < first + requestedWindowSize) {
					ds.startItem("hitgroup").startMap();
					ds	.entry("identity", group.getIdentity().serialize())
						.entry("identityDisplay", group.getIdentity().toString())
						.entry("size", group.size());
					ds.endMap().endItem();
				}

				i++;
			}
			ds.endList().endEntry();
			ds.endMap();

			return HTTP_OK;
		} finally {
			search.decrRef();
		}
	}

}
