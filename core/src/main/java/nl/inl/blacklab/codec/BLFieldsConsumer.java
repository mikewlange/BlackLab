package nl.inl.blacklab.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/**
 * BlackLab FieldsConsumer: writes postings information to the index,
 * using a delegate and extending its functionality by also writing a forward
 * index.
 *
 * Adapted from <a href="https://github.com/meertensinstituut/mtas/">MTAS</a>.
 */
public class BLFieldsConsumer extends FieldsConsumer {

    private static final String TERMS_EXT = "terms";

    private static final String TERMVEC_TMP_EXT = "termvec.tmp";

    protected static final Logger logger = LogManager.getLogger(BLFieldsConsumer.class);

    /** The delegate whose functionality we're extending */
    private FieldsConsumer delegateFieldsConsumer;

    /** Current segment write state, used when writing to the index */
    private SegmentWriteState state;

    /** The posting format's name */
    @SuppressWarnings("unused")
    private String postingFormatName;

    /** Name of the posting format we've extended */
    private String delegatePostingsFormatName;

    public BLFieldsConsumer(FieldsConsumer fieldsConsumer, SegmentWriteState state, String name,
            String delegatePostingsFormatName) {
        this.delegateFieldsConsumer = fieldsConsumer;
        this.state = state;
        this.postingFormatName = name;
        this.delegatePostingsFormatName = delegatePostingsFormatName;
    }

//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.lucene.codecs.FieldsConsumer#merge(org.apache.lucene.index.
//     * MergeState)
//     */
//    @Override
//    public void merge(MergeState mergeState) throws IOException {
//        final List<Fields> fields = new ArrayList<>();
//        final List<ReaderSlice> slices = new ArrayList<>();
//
//        int docBase = 0;
//
//        for (int readerIndex = 0; readerIndex < mergeState.fieldsProducers.length; readerIndex++) {
//            final FieldsProducer f = mergeState.fieldsProducers[readerIndex];
//
//            final int maxDoc = mergeState.maxDocs[readerIndex];
//            f.checkIntegrity();
//            slices.add(new ReaderSlice(docBase, maxDoc, readerIndex));
//            fields.add(f);
//            docBase += maxDoc;
//        }
//
//        Fields mergedFields = new MappedMultiFields(mergeState,
//                new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
//                        slices.toArray(ReaderSlice.EMPTY_ARRAY)));
//        write(mergedFields);
//    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.FieldsConsumer#write(org.apache.lucene.index.
     * Fields )
     */
    @Override
    public void write(Fields fields) throws IOException {
        // Use delegate to write most of the postings information.
        delegateFieldsConsumer.write(fields);

        // Write our postings extension information
        FieldInfos fieldInfos = state.fieldInfos;
        IndexOutput fieldsFile = null;
        IndexOutput termIndexFile = null;
        try {
            fieldsFile = openOutputFile("fields");
            termIndexFile = openOutputFile("termindex");

            // First we write a temporary dump of the term vector, and keep track of
            // where we can find term occurrences per document so we can reverse this
            // file later.
            Map<String, Map<Integer, List<Long>>> docPosOffsetsPerField = new HashMap<>();
            IndexOutput tempTermVectorFile = null;
            IndexOutput termsFile = null;
            try {
                tempTermVectorFile = openOutputFile(TERMVEC_TMP_EXT);
                termsFile = openOutputFile(TERMS_EXT);
                tempTermVectorFile.writeString(delegatePostingsFormatName);

                // For each field...
                fieldsFile.writeInt(fields.size());
                for (String field: fields) {
                    Terms terms = fields.terms(field);
                    if (terms == null)
                        continue;

                    // See what attached information this field has
                    boolean hasPositions = terms.hasPositions();
                    boolean hasFreqs = terms.hasFreqs();
    //                boolean hasOffsets = terms.hasOffsets();
    //                FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
    //                boolean hasPayloads = fieldInfo.hasPayloads();

                    // If it's (part of) a complex field...
                    if (hasFreqs && hasPositions) {

                        // Record field name, offset into term index file, number of terms
                        fieldsFile.writeString(field);
                        fieldsFile.writeLong(termIndexFile.getFilePointer());
                        termIndexFile.writeLong(terms.size());

                        // Keep track of where to find term positions for each document
                        // (for reversing index)
                        Map<Integer, List<Long>> docPosOffsets = docPosOffsetsPerField.get(field);
                        if (docPosOffsets == null) {
                            docPosOffsets = new HashMap<>();
                            docPosOffsetsPerField.put(field, docPosOffsets);
                        }

                        // For each term in this field...
                        PostingsEnum postingsEnum = null; // we'll reuse this for efficiency
                        TermsEnum termsEnum = terms.iterator();
                        while (true) {
                            BytesRef term = termsEnum.next();
                            if (term == null)
                                break;
                            termIndexFile.writeLong(termsFile.getFilePointer()); // where to find term string
                            termsFile.writeString(term.utf8ToString());          // term string

                            // For each document containing this term...
                            postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.POSITIONS);
                            while (true) {
                                Integer docId = postingsEnum.nextDoc();
                                if (docId.equals(DocIdSetIterator.NO_MORE_DOCS))
                                    break;

                                // Keep track of term positions offsets in term vector file
                                List<Long> vectorFileOffsets = docPosOffsets.get(docId);
                                if (vectorFileOffsets == null) {
                                    vectorFileOffsets = new ArrayList<>();
                                    docPosOffsets.put(docId, vectorFileOffsets);
                                }
                                vectorFileOffsets.add(tempTermVectorFile.getFilePointer());

                                // For each occurrence of term in this doc...
                                int nOccurrences = postingsEnum.freq();
                                tempTermVectorFile.writeInt(nOccurrences);
                                for (int i = 0; i < nOccurrences; i++) {
                                    tempTermVectorFile.writeInt(postingsEnum.nextPosition());
                                }
                            }
                        }
                        // Store additional metadata about this field
                        fieldInfos.fieldInfo(field).putAttribute("funFactsAboutField", "didYouKnowThat?");
                    }
                }
            } finally {
                if (termsFile != null)
                    termsFile.close();
                if (tempTermVectorFile != null)
                    tempTermVectorFile.close();
            }
            // Reverse the reverse index to create forward index
            IndexInput inTermVectorFile = null;
            try {
                inTermVectorFile = openInputFile(TERMVEC_TMP_EXT);
                inTermsFile = openInputFile(TERMS_EXT);
                for (Entry<String, Map<Integer, List<Long>>> fieldEntry: docPosOffsetsPerField.entrySet()) {
                    String field = fieldEntry.getKey();
                    Map<Integer, List<Long>> docPosOffsets = fieldEntry.getValue();
                    for (Entry<Integer, List<Long>> docEntry: docPosOffsets.entrySet()) {
                        Integer docId = docEntry.getKey();
                        List<Long> termPosOffsets = docEntry.getValue();
                        for (Long offset: termPosOffsets) {
                            inTermsFile.seek(offset);
                            Integer docId = postingsEnum.nextDoc();
                            if (docId.equals(DocIdSetIterator.NO_MORE_DOCS))
                                break;

                            // Keep track of term positions offsets in term vector file
                            List<Long> vectorFileOffsets = docPosOffsets.get(docId);
                            if (vectorFileOffsets == null) {
                                vectorFileOffsets = new ArrayList<>();
                                docPosOffsets.put(docId, vectorFileOffsets);
                            }
                            vectorFileOffsets.add(tempTermVectorFile.getFilePointer());

                            // For each occurrence of term in this doc...
                            int nOccurrences = postingsEnum.freq();
                            tempTermVectorFile.writeInt(nOccurrences);
                            for (int i = 0; i < nOccurrences; i++) {
                                tempTermVectorFile.writeInt(postingsEnum.nextPosition());
                            }
                        }
                    }
                    // Store additional metadata about this field
                    fieldInfos.fieldInfo(field).putAttribute("funFactsAboutField", "didYouKnowThat?");
                }
            } finally {
                if (inTermVectorFile != null)
                    inTermVectorFile.close();
            }
        } finally {
            if (termIndexFile != null)
                termIndexFile.close();
            if (fieldsFile != null)
                fieldsFile.close();
        }
    }

    protected IndexOutput openOutputFile(String ext) throws IOException {
        return state.directory.createOutput(getSegmentFileName(ext), state.context);
    }

    protected IndexInput openInputFile(String ext) throws IOException {
        return state.directory.openInput(getSegmentFileName(ext), state.context);
    }

    protected String getSegmentFileName(String ext) {
        return IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "bl" + ext);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.FieldsConsumer#close()
     */
    @Override
    public void close() throws IOException {
        delegateFieldsConsumer.close();
    }

}
