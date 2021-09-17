package uk.gov.gchq.gaffer.federatedstore;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BugHuntTest {

    public static final String TSTV = TypeSubTypeValue.class.getSimpleName();
    public static final String A = "a";
    public static final String AA = "aa";
    public static final String AAA = "aaa";
    public static final String B = "b";
    public static final String BB = "bb";
    public static final String BBB = "bbb";
    public static final Context CONTEXT = new Context(new User("user"));
    public static final TypeSubTypeValue SOURCE = new TypeSubTypeValue(A, AA, AAA);
    public static final TypeSubTypeValue DEST = new TypeSubTypeValue(B, BB, BBB);
    public static final String TEST_EDGE = "testEdge";
    public static final String EXPECTED = "{\n" +
            "  \"class\" : \"uk.gov.gchq.gaffer.data.element.Edge\",\n" +
            "  \"group\" : \"testEdge\",\n" +
            "  \"source\" : {\n" +
            "    \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\" : {\n" +
            "      \"type\" : \"a\",\n" +
            "      \"subType\" : \"aa\",\n" +
            "      \"value\" : \"aaa\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"destination\" : {\n" +
            "    \"uk.gov.gchq.gaffer.types.TypeSubTypeValue\" : {\n" +
            "      \"type\" : \"b\",\n" +
            "      \"subType\" : \"bb\",\n" +
            "      \"value\" : \"bbb\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"directed\" : false,\n" +
            "  \"matchedVertex\" : \"SOURCE\",\n" +
            "  \"properties\" : { }\n" +
            "}";
    public static final Edge EDGE = new Edge.Builder()
                .group(TEST_EDGE)
                .source(SOURCE)
                .dest(DEST)
                .build();

    @Test
    public void testGetAllElementsWithViewFromMap() throws Exception {
        MapStore store = getMapStore();

        addElement(store);

        CloseableIterable<? extends Element> results = store.execute(new GetAllElements.Builder()
                .view(new View.Builder().edge(TEST_EDGE).build())
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetAllElementsNoViewFromMap() throws Exception {
        MapStore store = getMapStore();

        addElement(store);

        CloseableIterable<? extends Element> results = store.execute(new GetAllElements.Builder()
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        // NullPointerException: No View - GetAllElementsHandler$AllElementsIterable.iterator
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetElementsWithEdgeSeedAndViewFromMap() throws Exception {
        MapStore store = getMapStore();

        addElement(store);

        CloseableIterable<? extends Element> results = store.execute(new GetElements.Builder()
                .input(new EdgeSeed(SOURCE, DEST))
                .view(new View.Builder().edge(TEST_EDGE).build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetElementsWithEdgeSeedNoViewFromMap() throws Exception {
        MapStore store = getMapStore();

        addElement(store);

        CloseableIterable<? extends Element> results = store.execute(new GetElements.Builder()
                .input(new EdgeSeed(SOURCE, DEST))
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        // NullPointerException: No View - GetElementsUtil.applyView
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetElementsWithEdgeAndViewFromMap() throws Exception {
        MapStore store = getMapStore();

        addElement(store);

        CloseableIterable<? extends Element> results = store.execute(new GetElements.Builder()
                .input(new Edge.Builder()
                        .group(TEST_EDGE)
                        .source(SOURCE)
                        .dest(DEST)
                        .build())
                .view(new View.Builder().edge(TEST_EDGE).build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        // java.lang.ClassCastException: uk.gov.gchq.gaffer.data.element.Edge cannot be cast to uk.gov.gchq.gaffer.operation.data.EdgeSeed
        // MapStore with Edge instead of EdgeSeed
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetElementsWithEdgeNoViewFromMap() throws Exception {
        MapStore store = getMapStore();

        addElement(store);

        CloseableIterable<? extends Element> results = store.execute(new GetElements.Builder()
                .input(new Edge.Builder()
                        .group(TEST_EDGE)
                        .source(SOURCE)
                        .dest(DEST)
                        .build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        // NullPointerException: No View - GetElementsUtil.applyView
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));

    }

    @Test
    public void testGetAllElementsWithViewFromAccumulo() throws Exception {
        AccumuloStore store = getAccumuloStore();

        addElement(store);

        CloseableIterable<? extends Element> results = store.execute(new GetAllElements.Builder()
                .view(new View.Builder().edge(TEST_EDGE).build())
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        // FAIL: Json result lacks a "matchedVertex"
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetAllElementsNoViewFromAccumulo() throws Exception {
        AccumuloStore store = getAccumuloStore();

        addElement(store);

        // NullPointerException: No View - ByteEntityIteratorSettingsFactory.getElementPropertyRangeQueryFilter
        CloseableIterable<? extends Element> results = store.execute(new GetAllElements.Builder()
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        // FAIL: Json result lacks a "matchedVertex"
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetElementsWithEdgeSeedAndViewFromAccumulo() throws Exception {
        AccumuloStore store = getAccumuloStore();

        addElement(store);

        CloseableIterable<? extends Element> results = store.execute(new GetElements.Builder()
                .input(new EdgeSeed(SOURCE, DEST))
                .view(new View.Builder().edge(TEST_EDGE).build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetElementsWithEdgeSeedNoViewFromAccumulo() throws Exception {
        AccumuloStore store = getAccumuloStore();

        addElement(store);

        // NullPointerException: No View - AbstractCoreKeyIteratorSettingsFactory.getElementPreAggregationFilterIteratorSetting
        CloseableIterable<? extends Element> results = store.execute(new GetElements.Builder()
                .input(new EdgeSeed(SOURCE, DEST))
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetElementsWithEdgeAndViewFromAccumulo() throws Exception {
        AccumuloStore store = getAccumuloStore();

        addElement(store);

        CloseableIterable<? extends Element> results = store.execute(new GetElements.Builder()
                .input(new Edge.Builder()
                        .group(TEST_EDGE)
                        .source(SOURCE)
                        .dest(DEST)
                        .build())
                .view(new View.Builder().edge(TEST_EDGE).build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    @Test
    public void testGetElementsWithEdgeNoViewFromAccumulo() throws Exception {
        AccumuloStore store = getAccumuloStore();

        addElement(store);

        // NullPointerException: No View - AbstractCoreKeyIteratorSettingsFactory.getElementPreAggregationFilterIteratorSetting
        CloseableIterable<? extends Element> results = store.execute(new GetElements.Builder()
                .input(new Edge.Builder()
                        .group(TEST_EDGE)
                        .source(SOURCE)
                        .dest(DEST)
                        .build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .build(), CONTEXT);

        Assert.assertNotNull(results);
        Assert.assertTrue(results.iterator().hasNext());

        final List list = Streams.toStream(results).collect(Collectors.toList());

        Assert.assertEquals(1, list.size());
        Assert.assertEquals(EDGE, list.get(0));
        Assert.assertEquals(EXPECTED, new String(JSONSerialiser.serialise(list.get(0), true)));
    }

    private MapStore getMapStore() throws StoreException {
        MapStore store = new MapStore();

        store.initialise("store1", getSchema(), new MapStoreProperties());
        return store;
    }

    private AccumuloStore getAccumuloStore() throws StoreException {
        AccumuloStore store = new SingleUseMockAccumuloStore();

        store.initialise("store1", getSchema(), new AccumuloProperties());
        return store;
    }

    private Schema getSchema() throws StoreException {
        return new Schema.Builder()
                .edge(TEST_EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TSTV)
                        .destination(TSTV)
                        .build())
                .type(TSTV, TypeSubTypeValue.class)
                .build();
    }

    private void addElement(final Store store) throws OperationException {
        store.execute(new AddElements.Builder()
                .input(EDGE)
                .build(), CONTEXT);
    }

}
