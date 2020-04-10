package datawave.query.tables;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

public class DedupingIteratorTest {
    
    private List<Map.Entry<Key,Value>> list;
    
    @Before
    public void before() {
        list = new ArrayList<>(1160);
        // dups in the front
        for (int i = 1; i <= 20; i++) {
            list.add(new TestEntry(new Key(new Text("Key" + i)), null));
            list.add(new TestEntry(new Key(new Text("Key" + i)), null));
            list.add(new TestEntry(new Key(new Text("Key" + i)), null));
            list.add(new TestEntry(new Key(new Text("Key" + i)), null));
        }
        // unique in the middle
        for (int i = 21; i <= 900; i++) {
            list.add(new TestEntry(new Key(new Text("Key" + i)), null));
        }
        // some dups on the end
        for (int i = 901; i <= 1000; i++) {
            list.add(new TestEntry(new Key(new Text("Key" + i)), null));
            list.add(new TestEntry(new Key(new Text("Key" + i)), null));
        }
    }
    
    @Test(expected = IllegalStateException.class)
    public void test_debug_1in1Q_error() {
        boolean debug = true;
        int bloomExpected = 20;
        double bloomFpp = 1e-15;
        
        assertEquals(1160, list.size());
        
        Iterable<Map.Entry<Key,Value>> nodups = () -> new DedupingIterator(list.iterator(), bloomExpected, bloomFpp, debug);
        // Should throw the 1 in 1Q error
        StreamSupport.stream(nodups.spliterator(), false).count();
    }
    
    @Test()
    public void test_nodebug_nodups() {
        boolean debug = false;
        int bloomExpected = 1000;
        double bloomFpp = 1e-15;
        
        assertEquals(1160, list.size());
        
        Iterable<Map.Entry<Key,Value>> in = () -> new DedupingIterator(list.iterator(), bloomExpected, bloomFpp, debug);
        Set<Map.Entry<Key,Value>> out = new HashSet<>();
        in.forEach(out::add);
        assertEquals(1000, out.size());
        
    }
    
    private static class TestEntry implements Map.Entry<Key,Value> {
        
        private Key key;
        private Value val;
        
        TestEntry(Key key, Value val) {
            this.key = key;
            this.val = val;
        }
        
        @Override
        public Key getKey() {
            return this.key;
        }
        
        @Override
        public Value getValue() {
            return this.val;
        }
        
        @Override
        public Value setValue(Value val) {
            this.val = val;
            return this.val;
        }
    }
    
}
