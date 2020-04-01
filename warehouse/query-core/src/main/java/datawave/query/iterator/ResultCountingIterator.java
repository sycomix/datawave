package datawave.query.iterator;

import com.google.common.collect.Maps;
import datawave.data.type.util.NumericalEncoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.hadoop.io.Text;

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.log4j.Logger;

/**
 * Created on 9/6/16.
 */
public class ResultCountingIterator implements Iterator<Entry<Key,Value>> {
    private volatile long resultCount = 0;
    private Iterator<Entry<Key,Value>> serializedDocuments = null;
    private YieldCallback<Key> yield;
    private static Logger log = Logger.getLogger(ResultCountingIterator.class);
    
    public ResultCountingIterator(Iterator<Entry<Key,Value>> serializedDocuments, long resultCount, YieldCallback<Key> yieldCallback) {
        this.serializedDocuments = serializedDocuments;
        this.resultCount = resultCount;
        this.yield = yieldCallback;
    }
    
    @Override
    public boolean hasNext() {
        boolean hasNext = serializedDocuments.hasNext();
        if (yield != null && yield.hasYielded()) {
            yield.yield(addKeyCount(yield.getPositionAndReset()));
        }
        return hasNext;
    }
    
    @Override
    public Entry<Key,Value> next() {
        Entry<Key,Value> next = serializedDocuments.next();
        if (yield != null && yield.hasYielded()) {
            log.warn("Yield unexpectadly detected in a next");
        }
        if (next != null) {
            next = Maps.immutableEntry(addKeyCount(next.getKey()), next.getValue());
        }
        return next;
    }
    
    private Key addKeyCount(Key key) {
        resultCount++;
        return new Key(key.getRow(), new Text(NumericalEncoder.encode(Long.toString(resultCount)) + '\0' + key.getColumnFamily()), key.getColumnQualifier(),
                        key.getColumnVisibility(), key.getTimestamp());
    }
    
    @Override
    public void remove() {
        serializedDocuments.remove();
    }
}
