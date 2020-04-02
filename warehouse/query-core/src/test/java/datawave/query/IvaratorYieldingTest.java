package datawave.query;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.QueryIterator;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.TypeMetadata;
import datawave.query.util.TypeMetadataHelper;
import datawave.query.util.TypeMetadataWriter;
import datawave.query.util.WiseGuysIngest;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import javax.inject.Inject;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.iterators.YieldingKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static datawave.query.QueryTestTableHelper.METADATA_TABLE_NAME;
import static datawave.query.QueryTestTableHelper.MODEL_TABLE_NAME;
import static datawave.query.QueryTestTableHelper.SHARD_INDEX_TABLE_NAME;
import static datawave.query.QueryTestTableHelper.SHARD_TABLE_NAME;
import static datawave.query.iterator.QueryOptions.SORTED_UIDS;

public abstract class IvaratorYieldingTest {
    private static final Logger log = Logger.getLogger(IvaratorYieldingTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    private Set<Authorizations> authSet = Collections.singleton(auths);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    
    private KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
    
    @Deployment
    public static JavaArchive createDeployment() {
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class)
                        .deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @Before
    public void setup() throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        logic.setFullTableScanEnabled(true);
        // this should force regex expansion into ivarators
        logic.setMaxValueExpansionThreshold(1);
        
        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());
        
        // setup a directory for cache results
        File tmpDir = this.tmpDir.newFolder("Ivarator.cache");
        logic.setIvaratorCacheBaseURIs(tmpDir.toURI().toString());
        
        deserializer = new KryoDocumentDeserializer();
    }
    
    protected abstract void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParams) throws Exception;
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends IvaratorYieldingTest {
        protected static Connector connector = null;
        private static final String tempDirForIvaratorInterruptTest = "/tmp/TempDirForIvaratorInterruptShardRangeTest";
        
        @BeforeClass
        public static void setUp() throws Exception {
            // this will get property substituted into the TypeMetadataBridgeContext.xml file
            // for the injection test (when this unit test is first created)
            System.setProperty("type.metadata.dir", tempDirForIvaratorInterruptTest);
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log, RebuildingScannerTestHelper.TEARDOWN.ALWAYS_SANS_CONSISTENCY,
                            RebuildingScannerTestHelper.INTERRUPT.FI_EVERY_OTHER);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, METADATA_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        
        @AfterClass
        public static void teardown() {
            // maybe delete the temp folder here
            File tempFolder = new File(tempDirForIvaratorInterruptTest);
            if (tempFolder.exists()) {
                try {
                    FileUtils.forceDelete(tempFolder);
                } catch (IOException ex) {
                    log.error(ex);
                }
            }
            TypeRegistry.reset();
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends IvaratorYieldingTest {
        protected static Connector connector = null;
        private static final String tempDirForIvaratorInterruptTest = "/tmp/TempDirForIvaratorInterruptDocumentRangeTest";
        
        @BeforeClass
        public static void setUp() throws Exception {
            // this will get property substituted into the TypeMetadataBridgeContext.xml file
            // for the injection test (when this unit test is first created)
            System.setProperty("type.metadata.dir", tempDirForIvaratorInterruptTest);
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log, RebuildingScannerTestHelper.TEARDOWN.ALWAYS_SANS_CONSISTENCY,
                            RebuildingScannerTestHelper.INTERRUPT.FI_EVERY_OTHER);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, METADATA_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        
        @AfterClass
        public static void teardown() {
            // maybe delete the temp folder here
            File tempFolder = new File(tempDirForIvaratorInterruptTest);
            if (tempFolder.exists()) {
                try {
                    FileUtils.forceDelete(tempFolder);
                } catch (IOException ex) {
                    log.error(ex);
                }
            }
            TypeRegistry.reset();
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, Connector connector)
                    throws Exception {
        log.debug("runTestQuery");
        log.trace("Creating QueryImpl");
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(querystr);
        settings.setParameters(extraParms);
        settings.setId(UUID.randomUUID());
        
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());
        logic.setMaxEvaluationPipelines(1);
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        
        TypeMetadataWriter typeMetadataWriter = TypeMetadataWriter.Factory.createTypeMetadataWriter();
        TypeMetadataHelper typeMetadataHelper = new TypeMetadataHelper.Factory().createTypeMetadataHelper(connector, MODEL_TABLE_NAME, authSet, false);
        Map<Set<String>,TypeMetadata> typeMetadataMap = typeMetadataHelper.getTypeMetadataMap(authSet);
        typeMetadataWriter.writeTypeMetadataMap(typeMetadataMap, MODEL_TABLE_NAME);
        
        HashSet<String> expectedSet = new HashSet<>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<>();
        Set<Document> docs = new HashSet<>();
        for (Map.Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            
            log.debug(entry.getKey() + " => " + d);
            
            Attribute<?> attr = d.get("UUID");
            if (attr == null)
                attr = d.get("UUID.0");
            
            Assert.assertNotNull("Result Document did not contain a 'UUID'", attr);
            Assert.assertTrue("Expected result to be an instance of DatwawaveTypeAttribute, was: " + attr.getClass().getName(), attr instanceof TypeAttribute
                            || attr instanceof PreNormalizedAttribute);
            
            TypeAttribute<?> UUIDAttr = (TypeAttribute<?>) attr;
            
            String UUID = UUIDAttr.getType().getDelegate().toString();
            Assert.assertTrue("Received unexpected UUID: " + UUID, expected.contains(UUID));
            
            resultSet.add(UUID);
            docs.add(d);
        }
        
        if (expected.size() > resultSet.size()) {
            expectedSet.addAll(expected);
            expectedSet.removeAll(resultSet);
            
            for (String s : expectedSet) {
                log.warn("Missing: " + s);
            }
        }
        
        if (!expected.containsAll(resultSet)) {
            log.error("Expected results " + expected + " differ form actual results " + resultSet);
        }
        Assert.assertTrue("Expected results " + expected + " differ form actual results " + resultSet, expected.containsAll(resultSet));
        Assert.assertEquals("Unexpected number of records", expected.size(), resultSet.size());
    }
    
    @Test
    public void testIvaratorInterruptedAndYieldSorted() throws Exception {
        Map<String,String> params = new HashMap<>();
        
        // both required in order to force ivarator to call fillSets
        params.put(SORTED_UIDS, "true");
        logic.getConfig().setUnsortedUIDsEnabled(false);
        String query = "UUID =~ '^[CS].*' && filter:includeRegex(UUID, '.*A.*')";
        String[] results = new String[] {"SOPRANO", "CAPONE"}; // should skip "CORLEONE"
        logic.setYieldThresholdMs(1);
        logic.getQueryPlanner().setQueryIteratorClass(YieldingQueryIterator.class);
        runTestQuery(Arrays.asList(results), query, format.parse("20091231"), format.parse("20150101"), params);
    }


    @Test
    public void testIvaratorInterruptedAndYieldUnsorted() throws Exception {
        String query = "UUID =~ '^[A-Z].*' && filter:includeRegex(UUID, '.*S.*')";
        String[] results = new String[] {"SOPRANO"}; // should skip "CORLEONE", "CAPONE"
        logic.setYieldThresholdMs(1);
        logic.getQueryPlanner().setQueryIteratorClass(YieldingQueryIterator.class);
        runTestQuery(Arrays.asList(results), query, format.parse("20091231"), format.parse("20150101"), Collections.EMPTY_MAP);
    }

    public static class YieldingQueryIterator implements YieldingKeyValueIterator<Key, Value> {

        private QueryIterator __delegate;
        private YieldCallback<Key> __yield = new YieldCallback<>();
        private SortedKeyValueIterator<Key,Value> __source;
        private Map<String,String>__options;
        private IteratorEnvironment __env;
        private Range __range;
        private Collection<ByteSequence> __columnFamilies;
        private boolean __inclusive;


        public YieldingQueryIterator() {
            __delegate = new QueryIterator();
        }

        public YieldingQueryIterator(QueryIterator other, IteratorEnvironment env) {
            __delegate = new QueryIterator(other, env);
        }

        @Override public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
            __source = source;
            __options = options;
            __env = env;
            __delegate.init(source, options, env);
            // now enable yielding
            __delegate.enableYielding(__yield);
        }

        @Override
        public boolean hasTop() {
            return __delegate.hasTop();
        }

        @Override
        public void enableYielding(YieldCallback<Key> yieldCallback) {
            throw new UnsupportedOperationException("Yielding being handled internally");
        }

        @Override public void next() throws IOException {
            __delegate.next();
            while (__yield.hasYielded()) {
                __delegate = new QueryIterator();
                __delegate.init(__source, __options, __env);
                __delegate.enableYielding(__yield);
                Range r = new Range(__yield.getPositionAndReset(), false, __range.getEndKey(), __range.isEndKeyInclusive());
                log.info("Yielded at " + r.getStartKey());
                __delegate.seek(r, __columnFamilies, __inclusive);
            }
        }

        @Override public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            __range = range;
            __columnFamilies = columnFamilies;
            __inclusive = inclusive;
            __delegate.seek(range, columnFamilies, inclusive);
            while (__yield.hasYielded()) {
                __delegate = new QueryIterator();
                __delegate.init(__source, __options, __env);
                __delegate.enableYielding(__yield);
                Range r = new Range(__yield.getPositionAndReset(), false, __range.getEndKey(), __range.isEndKeyInclusive());
                log.info("Yielded at " + r.getStartKey());
                __delegate.seek(r, __columnFamilies, __inclusive);
            }
        }

        @Override public Key getTopKey() {
            return __delegate.getTopKey();
        }

        @Override public Value getTopValue() {
            return __delegate.getTopValue();
        }

        @Override public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
            return __delegate.deepCopy(env);
        }

    }
}
