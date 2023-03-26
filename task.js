import fs from 'fs';
import ETL from '@tak-ps/etl';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(fs.readFileSync(dotfile)));
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task extends ETL {
    static schema() {
        return {
            type: 'object',
            required: ['ADSBX_TOKEN'],
            properties: {
                'ADSBX_TOKEN': {
                    type: 'string',
                    description: 'API Token for ADSBExachange'
                },
                'ADSBX_INCLUDES': {
                    type: 'array',
                    display: 'table',
                    description: 'Limit resultant features to a given list of ids',
                    items: {
                        type: 'object',
                        properties: {
                            domain: {
                                type: 'string',
                                description: 'Public Safety domain of the Aircraft',
                                enum: ['EMS', 'FIRE', 'LAW']
                            },
                            agency: {
                                type: 'string',
                                description: 'Agency in control of the Aircraft'
                            },
                            callsign: {
                                type: 'string',
                                description: 'Callsign of the Aircraft'
                            },
                            registration: {
                                type: 'string',
                                description: 'Registration Number of the Aircraft'
                            },
                            type: {
                                type: 'string',
                                description: 'Type of Aircraft',
                                enum: [
                                    'HELICOPTER',
                                    'FIXED WING'
                                ]
                            },
                            icon: {
                                type: 'string',
                                description: 'Optional TAK Custom Icon'
                            }
                        }
                    }
                },
                'DEBUG': {
                    type: 'boolean',
                    default: false,
                    description: 'Print ADSBX results in logs'
                }
            }
        };
    }

    async control() {
        const layer = await this.layer();

        if (!layer.environment.ADSBX_TOKEN) throw new Error('No ADSBX API Token Provided');
        if (!layer.environment.ADSBX_INCLUDES) layer.environment.ADSBX_INCLUDES = '[]';

        const token = layer.environment.ADSBX_TOKEN;
        const includes = layer.environment.ADSBX_INCLUDES;
        const api = 'https://adsbexchange.com/api/aircraft/v2/lat/40.14401/lon/-119.81204/dist/2650/';

        const url = new URL(api);
        url.searchParams.append('apiKey', token);
        url.searchParams.append('cacheBuster', +new Date());

        const res = await fetch(url, {
            headers: {
                'api-auth': token
            }
        });

        const ids = new Map();

        for (const ac of (await res.json()).ac) {
            if (!ac.flight && !ac.r) continue;

            const id = (ac.r || ac.flight).toLowerCase().trim();
            const coordinates = [ac.lon, ac.lat];

            // If alt. is present convert to meters
            if (!isNaN(parseInt(ac.alt_geom))) coordinates.push(ac.alt_geom * 0.3048);

            if (!id.trim().length) continue;

            ids.set(id, {
                id: id,
                type: 'Feature',
                properties: {
                    type: 'a-f-A',
                    registration: (ac.r || '').trim(),
                    callsign: (ac.flight || '').trim(),
                    time: new Date(),
                    start: new Date(),
                    squak: ac.squak,
                    emergency: ac.emergency,
                    speed: ac.gs * 0.514444 || 9999999.0,
                    course: ac.track || 9999999.0
                },
                geometry: {
                    type: 'Point',
                    coordinates
                }
            });
        }

        const features = [];
        const features_ids = new Set();
        for (const include of includes) {
            const id = include.registration.toLowerCase().trim();

            if (ids.has(id)) {
                const feat = ids.get(id);
                if (include.type === 'HELICOPTER') feat.properties.type = 'a-f-A-C-H';
                if (include.type === 'FIXED WING') feat.properties.type = 'a-f-A-C-F';

                if (include.callsign) feat.properties.callsign = include.callsign;

                if (include.icon) {
                    feat.properties.icon = include.icon;
                } else if (include.type === 'HELICOPTER' && include.domain === 'EMS') {
                    feat.properties.icon = '66f14976-4b62-4023-8edb-d8d2ebeaa336/Public Safety Air/EMS_ROTOR.png'
                } else if (include.type === 'HELICOPTER' && include.domain === 'FIRE') {
                    feat.properties.icon = '66f14976-4b62-4023-8edb-d8d2ebeaa336/Public Safety Air/FIRE_ROTOR.png'
                } else if (include.type === 'HELICOPTER' && include.domain === 'LAW') {
                    feat.properties.icon = '66f14976-4b62-4023-8edb-d8d2ebeaa336/Public Safety Air/LE_ROTOR.png'
                } else if (include.type === 'FIXED WING' && include.domain === 'EMS') {
                    feat.properties.icon = '66f14976-4b62-4023-8edb-d8d2ebeaa336/Public Safety Air/EMS_FIXED_WING.png'
                } else if (include.type === 'FIXED WING' && include.domain === 'FIRE') {
                    feat.properties.icon = '66f14976-4b62-4023-8edb-d8d2ebeaa336/Public Safety Air/FIRE_AIR_ATTACK.png'
                } else if (include.type === 'FIXED WING' && include.domain === 'LAW') {
                    feat.properties.icon = '66f14976-4b62-4023-8edb-d8d2ebeaa336/Public Safety Air/LE_FIXED_WING.png'
                }

                if (feat.properties.callsign === 'FIREBIRD09-PPD') console.log(new Date(), feat.geometry.coordinates)

                if (!features_ids.has(id)) {
                    features_ids.add(id);
                    features.push(feat);
                }
            }
        }

        console.log(`ok - fetched ${ids.size} planes`);
        const fc = {
            type: 'FeatureCollection',
            features
        };

        await this.submit(fc);

        /*
        const knownres = await fetch(new URL(`/api/layer/${this.etl.layer}/query`, this.etl.api), {
            method: 'GET',
            headers: {
                Authorization: `bearer ${this.etl.token}`
            }
        });

        const known = await knownres.json();

        //TODO: Implement
        console.log(`ok - comparing against ${known.features.length} planes`);
        */
    }
}

export async function handler(event = {}) {
    if (event.type === 'schema') {
        return Task.schema();
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();
