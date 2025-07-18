import { Static, Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl'
import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType, InputFeatureCollection } from '@tak-ps/etl';

const Env = Type.Object({
    'Query LatLon': Type.String({
        description: 'Lat, Lon value to use for centering the API request',
        default: '40.14401,-119.81204'
    }),
    'Query Dist': Type.String({
        description: 'Distance from the provided Lat, Lon location in nautical miles (NM) to provide results',
        default: "2650"
    }),
    'ADSBX_API': Type.String({
        enum: [
            'https://adsbexchange-com1.p.rapidapi.com',
            'https://adsbexchange.com/api/aircraft'
        ],
        default: 'https://adsbexchange.com/api/aircraft'
    }),
    'ADSBX_TOKEN': Type.String({ description: 'API Token for ADSBExchange' }),
    'ADSBX_INCLUDES_FILTERING': Type.Boolean({
        description: 'Only show aircraft from the ADSBX_INCLUDES list. This is useful for filtering out large amounts of aircraft in an area.',
        default: true
    }),
    'ADSBX_INCLUDES_ICON': Type.Boolean({ 
        description: 'Change aircraft icon based on the group provided in ADSBX_INCLUDES, even when filtering is disabled.',
        default: true
    }),
    'ADSBX_INCLUDES': Type.Array(Type.Object({
        domain: Type.String({
            description: 'Public Safety domain of the Aircraft',
            enum: ['EMS', 'FIRE', 'LAW', 'FED', 'MIL'],
        }),
        callsign: Type.Optional(Type.String({ description: 'Callsign of the Aircraft.' })),
        registration: Type.Optional(Type.String({ description: 'Registration Number of the Aircraft.' })),
        group: Type.String({
            description: 'Category of Aircraft. This is used to determine the icon to use for the aircraft.',
            default: 'UNKNOWN',
            enum: [
                'UNKNOWN',
                'a-f-A-M-F-A',
                'a-f-A-M-F-C',
                'a-f-A-M-F-J',
                'a-f-A-M-F-O',
                'a-f-A-M-F-Q',
                'a-f-A-M-F-R-Z',
                'a-f-A-M-F-R',
                'a-f-A-M-F-U',
                'a-f-A-M-F-V',
                'a-f-A-M-F-WX',
                'a-f-A-M-F-Y',
                'a-f-A-M-H-H',
                'a-f-A-M-H-R',
                'a-f-A-M-H-V',
                'a-f-A-M-H',
                'a-n-A-M-F-V',
                'CIV_FIXED_CAP',
                'CIV_FIXED_ISR',
                'CIV_LTA_AIRSHIP',
                'CIV_LTA_BALLOON',
                'CIV_LTA_TETHERED',
                'CIV_ROTOR_ISR',
                'CIV_UAS',
                'CIV_UAS_ROTOR',
                'EMS_FIXED_WING',
                'EMS_ROTOR',
                'EMS_ROTOR_RESCUE',
                'FIRE_AIR_ATTACK',
                'FIRE_AIR_TANKER',
                'FIRE_INTEL',
                'FIRE_LEAD_PLANE',
                'FIRE_MULTI_USE',
                'FIRE_ROTOR',
                'FIRE_ROTOR_AIR_ATTACK',
                'FIRE_ROTOR_INTEL',
                'FIRE_ROTOR_RESCUE',
                'FIRE_SEAT',
                'FIRE_SMOKE_JMPR',
                'FIRE_UAS',
                'LE_FIXED_WING',
                'LE_FIXED_WING_ISR',
                'LE_ROTOR',
                'LE_ROTOR_RESCUE',
                'LE_UAS',
                'FED_FIXED_WING',
                'FED_FIXED_WING_ISR',
                'FED_ROTOR',
                'FED_ROTOR_RESCUE',
                'FED_UAS',
                'MIL_ROTOR_MED_RESCUE',
                'MIL_ROTOR_ISR_RESCUE'
            ]
        }),
    })),
    'ADSBX_EMERGENCY_HOSTILE': Type.Boolean({ 
        description: 'Mark flights in status "emergency" as "hostile". This allows them to appear in red on a TAK map.', 
        default: false 
    }),
    'ADSBX_Ignore_Tower_Vehicles': Type.Boolean({
        description: 'Ignore tower vehicles (TWR) and ground vehicles (GND).',
        default: true
    }),
    'ADSBX_ICAOHex_Domestic_Start': Type.String({ 
        description: 'ICAO HEX start value for domestic flights. E.g. A00000 for USA.', 
        default: 'A00000'
    }),
    'ADSBX_ICAOHex_Domestic_End': Type.String({ 
        description: 'ICAO HEX start value for domestic flights. E.g. AFFFFF for USA.', 
        default: 'AFFFFF'
    }),
    'DEBUG': Type.Boolean({ 
        description: 'Print ADSBX results in logs.', 
        default: false })
});

const ADSBResponse = Type.Object({
    hex: Type.String(),
    type: Type.String(),
    group: Type.Optional(Type.String({
        default: 'None',
        description: 'Provided by the join with ADSBX_INCLUDES items'
    })),
    flight: Type.Optional(Type.String()),
    r: Type.Optional(Type.String()),
    t: Type.Optional(Type.String()),
    dbFlags: Type.Optional(Type.Number()),
    alt_baro: Type.Optional(Type.Union([Type.Number(), Type.String()])),
    alt_geom: Type.Optional(Type.Number()),
    gs: Type.Optional(Type.Number()),
    track: Type.Optional(Type.Number()),
    baro_rate: Type.Optional(Type.Number()),
    squawk: Type.Optional(Type.String()),
    emergency: Type.Optional(Type.String()),
    category: Type.Optional(Type.String()),
    nav_qnh: Type.Optional(Type.Number()),
    nav_altitude_mcp: Type.Optional(Type.Number()),
    nav_heading: Type.Optional(Type.Number()),
    lat: Type.Number(),
    lon: Type.Number(),
    seen_pos: Type.Number(),
    seen: Type.Number(),
    dst: Type.Optional(Type.Number()),
})

export default class Task extends ETL {
    static name = 'etl-adsbx'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Env;
            } else {
                return ADSBResponse;
            }
        } else {
            return Type.Object({});
        }
    }

    async control() {
        const env = await this.env(Env);

        const api = `${env.ADSBX_API}/v2/lat/${env['Query LatLon'].split(',')[0].trim()}/lon/${env['Query LatLon'].split(',')[1].trim()}/dist/${env['Query Dist']}/`;

        const url = new URL(api);
        url.searchParams.append('apiKey', env.ADSBX_TOKEN);
        url.searchParams.append('cacheBuster', String(new Date().getTime()));

        const res = await fetch(url, {
            headers: {
                'x-rapidapi-key': env.ADSBX_TOKEN,
                'api-auth': env.ADSBX_TOKEN
            }
        });

        const body = await res.typed(Type.Object({
            msg: Type.String(),
            ac: Type.Array(ADSBResponse)
        }));

        const ids = new Map();

        for (const ac of body.ac) {
            if (!ac.flight && !ac.r) continue;

            if (env.ADSBX_Ignore_Tower_Vehicles && (ac.r == 'TWR' || ac.r == 'GND' || ac.type == 'adsb_icao_nt' )) continue; // Ignore tower, ground vehicles and test equipment

            const id = (ac.r || ac.flight).toLowerCase().trim();
            const coordinates = [ac.lon, ac.lat];

            // If alt. is present convert to meters
            if (ac.alt_geom) coordinates.push(ac.alt_geom * 0.3048);

            if (!id.trim().length) continue;

            // Determin the type of aircraft (fixed wing, rotorcraft, airship/balloon, etc.)
            // https://www.adsbexchange.com/emitter-category-ads-b-do-260b-2-2-3-2-5-2/
            let ac_type = ''; // Unknown
            switch (ac.category) {
                case 'A0':  // No ADS-B emitter category information. Still used for some airplanes.
                case 'A1':  // Light (< 15500 lbs) fixed wing aircraft
                case 'A2':  // Small (15500-75000 lbs) fixed wing aircraft
                case 'A3':  // Large (75000 to 300000 lbs) fixed wing aircraft
                case 'A4':  // High vortex large (aircraft such as B-757) fixed wing aircraft
                case 'A5':  // Heavy (> 300000 lbs) fixed wing aircraft
                case 'A6':  // High performance (> 5g acceleration and 400 kts) fixed wing aircraft
                    ac_type = '-F'; // Fixed Wing
                    break;
                case 'A7':
                    ac_type = '-H'; // Rotorcraft – Any rotorcraft regardless of weight.
                    break;
                case 'B2':
                    ac_type = '-L'; // Lighter-than-air – Any lighter than air (airship or balloon) regardless of weight.
                    break;
                default:
                    break;
            }

            // Determine whether the aircraft is a domestic or foreign flight
            // Based on the ICAO Hex code, which is a 6-character alphanumeric code assigned to each aircraft
            // https://www.aerotransport.org/html/ICAO_hex_decode.html
            let ac_affiliation = '-f'; // Friendly (Local)
            if (ac.hex.toLowerCase().trim() >= env.ADSBX_ICAOHex_Domestic_Start.toLowerCase().trim() &&
                ac.hex.toLowerCase().trim() <= env.ADSBX_ICAOHex_Domestic_End.toLowerCase().trim()) {
                ac_affiliation = '-f'; // Friendly (Local civilian)
            } else {
                ac_affiliation = '-n'; // Neutral (Foreign civilian)
            }

            // Determine whether the aircraft is civilian or military
            // https://www.adsbexchange.com/version-2-api-wip/
            let ac_civmil = '-C'; // Civilian
            if (ac.dbFlags !== undefined && ac.dbFlags % 2 !== 0) {
                ac_civmil = '-M'; // Military
                if (ac.hex.toLowerCase().trim() >= env.ADSBX_ICAOHex_Domestic_Start.toLowerCase().trim() &&
                ac.hex.toLowerCase().trim() <= env.ADSBX_ICAOHex_Domestic_End.toLowerCase().trim()) {
                    ac_affiliation = '-f'; // Friendly (Local Military)
                } else {
                    ac_affiliation = '-u'; // Unknown (Foreign Military)
                }
            }

            // Determine whether the aircraft is in emergency mode (show in red aka. "hostile") or not
            // https://www.adsbexchange.com/version-2-api-wip/
            if (ac.emergency !== undefined && ac.emergency !== 'none' && env.ADSBX_EMERGENCY_HOSTILE) {
                ac_affiliation = '-h'; // Emergency
            }

            for (const include of env.ADSBX_INCLUDES) {
                const markup = include.registration.toLowerCase().trim();
                if (id == markup) {
                    ac.group = include.group;
                }
            }

            ids.set(id, {
                id: id,
                type: 'Feature',
                properties: {
                    type: 'a' + ac_affiliation + '-A' + ac_civmil + ac_type,
                    callsign: (ac.flight || '').trim(),
                    time: new Date(),
                    start: new Date(),
                    speed: ac.gs * 0.514444 || 0,
                    course: ac.track || 9999999.0,
                    metadata: ac,
                    remarks: [
                        'Flight: ' + (ac.flight || 'Unknown').trim(),
                        'Registration: ' + (ac.r || 'Unknown').trim(),
                        'Type: ' + (ac.t || 'Unknown').trim(),
                        'Category: ' + (ac.category || 'Unknown').trim(),
                        'Emergency: ' + (ac.emergency || 'Unknown').trim(),
                        'Squawk: ' + (ac.squawk || 'Unknown').trim(),
                        'Group: ' + (ac.group || 'None').replace(/_/g,"-").trim(),  // CloudTAK formats "xx_yy_zz" as "xxyyzz" with yy being italics
                    ].join('\n')
                },
                geometry: {
                    type: 'Point',
                    coordinates
                }
            });

            // If the aircraft has a group, set the icon based on the group from the 'Public Safety Air' icon set
            // https://tak.gov/public-safety-air-icons/
            // This is used to display different icons for different types of public safety aircraft    
            const feat = ids.get(id);
            if (ac.group && ac.group.trim() !== 'UNKNOWN' && ac.group.trim() !== 'None' && env.ADSBX_INCLUDES_ICON) {
                // If the group starts with 'a-', it's a military symbol code (e.g., a-f-A-M-F-R), so use it directly as the type
                // This allows proper military symbology to be displayed in TAK
                if (ac.group && ac.group.trim().startsWith('a-')) {
                    feat.properties.type = ac.group.trim();
                }
                feat.properties.icon = '66f14976-4b62-4023-8edb-d8d2ebeaa336/Public Safety Air/' + ac.group.trim() + '.png';
            }
        }

        const features = [];
        const features_ids = new Set();

        if (env.ADSBX_INCLUDES_FILTERING) {
            for (const include of env.ADSBX_INCLUDES) {
                const id = include.registration.toLowerCase().trim();

                if (ids.has(id)) {
                    const feat = ids.get(id);

                    if (include.callsign) {
                        feat.properties.callsign = include.callsign;
                    }

                    if (include.group) {
                        feat.properties.metadata.group = include.group;
                    }

                    if (!features_ids.has(id)) {
                        features_ids.add(id);
                        features.push(feat);
                    }
                }
            }
        } else {
            for (const feat of ids.values()) {

                if (!features_ids.has(feat.id)) {
                    features_ids.add(feat.id);
                    features.push(feat);
                }
            }
        }

        console.log(`ok - fetched ${ids.size} aircraft`);
        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features
        };

        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

