import test from 'node:test';
import assert from 'node:assert';
import { SchemaType, DataFlowType } from '@tak-ps/etl';

// task.ts calls Task.init() at module scope which requires an ETL environment,
// so these must be set before the dynamic import below
process.env.ETL_API = process.env.ETL_API || 'http://localhost:5001';
process.env.ETL_LAYER = process.env.ETL_LAYER || '1';
process.env.ETL_TOKEN = process.env.ETL_TOKEN || 'etl.test-token';

const { default: Task } = await import('../task.js');

test('Task static config', () => {
    assert.equal(Task.name, 'etl-adsbx');
    assert.deepEqual(Task.flow, [DataFlowType.Incoming]);
});

test('Incoming Input schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Incoming);

    assert.equal(schema.type, 'object');
    for (const key of [
        'Query_LatLon',
        'Query_Dist',
        'ADSBX_API',
        'ADSBX_Token',
        'ADSBX_Filtering',
        'ADSBX_Use_Icon',
        'ADSBX_Includes',
        'ADSBX_Emergency_Alert',
        'ADSBX_Ignore_Tower_Vehicles',
        'ADSBX_ICAOHex_Domestic_Start',
        'ADSBX_ICAOHex_Domestic_End',
        'ADSBX_FireFighting_Squawk',
        'ADSBX_Include_Below_Elevation',
        'ADSBX_Below_Elevation_Feet',
        'Supplementary_Feeds',
        'DEBUG'
    ]) {
        assert.ok(schema.properties[key], `Env schema missing property: ${key}`);
    }

    assert.equal(schema.properties.ADSBX_Filtering.type, 'boolean');
    assert.equal(schema.properties.ADSBX_Filtering.default, false);
    assert.equal(schema.properties.ADSBX_Use_Icon.type, 'boolean');
    assert.equal(schema.properties.ADSBX_Use_Icon.default, true);
    assert.equal(schema.properties.ADSBX_Include_Below_Elevation.type, 'boolean');
    assert.equal(schema.properties.ADSBX_Include_Below_Elevation.default, false);
    assert.equal(schema.properties.ADSBX_Below_Elevation_Feet.type, 'number');
    assert.equal(schema.properties.ADSBX_Below_Elevation_Feet.default, 18000);
});

test('Incoming Output schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Output, DataFlowType.Incoming);

    assert.equal(schema.type, 'object');
    assert.ok(schema.properties.hex);
    assert.ok(schema.properties.lat);
    assert.ok(schema.properties.lon);
    assert.ok(schema.properties.seen_pos);
    assert.ok(schema.properties.seen);
    assert.ok(schema.properties.included);
    assert.equal(schema.properties.included.type, 'boolean');
    assert.equal(schema.properties.included.default, false);
});

test('Outgoing flow is not provided', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Outgoing);

    assert.deepEqual(schema.properties, {});
});
