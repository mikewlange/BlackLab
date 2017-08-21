package nl.inl.blacklab.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * BlackLab Codec: a customization of Lucene's way of storing information in the index,
 * to accomodate our forward index and (optional) content store.
 *
 * Adapted from <a href="https://github.com/meertensinstituut/mtas/">MTAS</a>.
 */
public class BLCodec extends Codec {

    protected static final Logger logger = LogManager.getLogger(BLCodec.class);

    /** Our codec's name. */
    public static final String CODEC_NAME = "BLCodec";

    /** The codec we're basing this codec on. */
    Codec delegate;

    public BLCodec() {
        super(CODEC_NAME);
        delegate = null;
    }

    public BLCodec(String name, Codec delegate) {
        super(name);
        this.delegate = delegate;
    }

    /** If we don't have a delegate yet, use the default Codec. */
    private void initDelegate() {
        if (delegate == null) {
            delegate = Codec.getDefault();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.Codec#postingsFormat()
     */
    @Override
    public PostingsFormat postingsFormat() {
        initDelegate();
        if (delegate.postingsFormat() instanceof PerFieldPostingsFormat) {
            Codec defaultCodec = Codec.getDefault();
            PostingsFormat defaultPostingsFormat = defaultCodec.postingsFormat();
            if (defaultPostingsFormat instanceof PerFieldPostingsFormat) {
                defaultPostingsFormat = ((PerFieldPostingsFormat) defaultPostingsFormat)
                        .getPostingsFormatForField(null);
                if ((defaultPostingsFormat == null)
                        || (defaultPostingsFormat instanceof PerFieldPostingsFormat)) {
                    // fallback option
                    return new BLCodecPostingsFormat(PostingsFormat.forName("Lucene53"));
                }
                return new BLCodecPostingsFormat(defaultPostingsFormat);
            }
            return new BLCodecPostingsFormat(defaultPostingsFormat);
        }
        return new BLCodecPostingsFormat(delegate.postingsFormat());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.Codec#docValuesFormat()
     */
    @Override
    public DocValuesFormat docValuesFormat() {
        initDelegate();
        return delegate.docValuesFormat();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.Codec#storedFieldsFormat()
     */
    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        initDelegate();
        return delegate.storedFieldsFormat();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.Codec#termVectorsFormat()
     */
    @Override
    public TermVectorsFormat termVectorsFormat() {
        initDelegate();
        return delegate.termVectorsFormat();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.Codec#fieldInfosFormat()
     */
    @Override
    public FieldInfosFormat fieldInfosFormat() {
        initDelegate();
        return delegate.fieldInfosFormat();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.Codec#segmentInfoFormat()
     */
    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        initDelegate();
        return delegate.segmentInfoFormat();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.Codec#normsFormat()
     */
    @Override
    public NormsFormat normsFormat() {
        initDelegate();
        return delegate.normsFormat();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.Codec#liveDocsFormat()
     */
    @Override
    public LiveDocsFormat liveDocsFormat() {
        initDelegate();
        return delegate.liveDocsFormat();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lucene.codecs.Codec#compoundFormat()
     */
    @Override
    public CompoundFormat compoundFormat() {
        initDelegate();
        return delegate.compoundFormat();
    }

//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.lucene.codecs.Codec#pointsFormat()
//     */
//    @Override
//    public PointsFormat pointsFormat() {
//        initDelegate();
//        return delegate.pointsFormat();
//    }

}
