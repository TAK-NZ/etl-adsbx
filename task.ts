/**
 * ETL-ADSBX - Aircraft location data via ADS-B
 * 
 * This ETL task fetches aircraft location data from ADSBExchange and transforms it
 * into Cursor-on-Target (CoT) format suitable for display on TAK maps, with special 
 * handling for public safety aircraft.
 */

import { Static, Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl'
import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType, InputFeatureCollection } from '@tak-ps/etl';

/**
 * Constants used throughout the ETL task
 */
// Special value indicating unknown course in CoT format
const UNKNOWN_COURSE = Number.NaN; // Using NaN per CoT specification for unknown values

// Conversion factor from knots to meters per second (1 knot = 0.51444... m/s)
const KNOTS_TO_MPS = 0.5144444;

// Conversion factor from feet to meters
const FEET_TO_METERS = 0.3048;

/**
 * UUID and path for the Public Safety Air icon set in TAK
 * This is used to display specialized icons for different types of public safety aircraft
 * See: https://tak.gov/public-safety-air-icons/
 */
const PUBLIC_SAFETY_AIR_ICON_PATH = '66f14976-4b62-4023-8edb-d8d2ebeaa336/Public Safety Air/';

/**
 * Set of valid icon groups that can be used with the Public Safety Air icon set
 * This is used to validate icon paths before setting them
 */
const VALID_ICON_GROUPS = new Set([
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
]);

/**
 * Environment configuration schema for the ETL task
 * These parameters can be configured through the CloudTAK interface
 */
const Env = Type.Object({
    'Query_LatLon': Type.String({
        description: 'Lat, Lon value to use for centering the API request',
        default: '-41.29,174.78'
    }),
    'Query_Dist': Type.String({
        description: 'Distance from the provided Lat, Lon location in nautical miles (NM) to provide results',
        default: "750"
    }),
    'ADSBX_API': Type.String({
        enum: [
            'https://adsbexchange-com1.p.rapidapi.com',
            'https://adsbexchange.com/api/aircraft'
        ],
        default: 'https://adsbexchange.com/api/aircraft'
    }),
    'ADSBX_Token': Type.String({ description: 'API Token for ADSBExchange' }),
    'ADSBX_Filtering': Type.Boolean({
        description: 'Only show aircraft from the ADSBX_Includes list. This is useful for filtering out large amounts of aircraft in an area.',
        default: false
    }),
    'ADSBX_Use_Icon': Type.Boolean({ 
        description: 'Change aircraft icon based on the group provided in ADSBX_Includes, even when filtering is disabled.',
        default: true
    }),
    'ADSBX_Includes': Type.Array(Type.Object({
        domain: Type.String({
            description: 'Public Safety domain of the Aircraft',
            enum: ['EMS', 'FIRE', 'LAW', 'FED', 'MIL'],
        }),
        ICAO_hex: Type.Optional(Type.String({ description: 'ICAO hex code of the Aircraft.' })),
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
        type: Type.Optional(Type.String({ description: 'Custom CoT type. E.g. a-f-A-M-F-C-H' })),
        comments: Type.Optional(Type.String({ description: 'Additional comments.' })),
    })),
    'ADSBX_Emergency_Alert': Type.Boolean({
        description: 'Use alert attribute to highlight aircraft in emergency status',
        default: true
    }),
    'ADSBX_Ignore_Tower_Vehicles': Type.Boolean({
        description: 'Ignore tower vehicles (TWR) and ground vehicles (GND).',
        default: true
    }),
    'ADSBX_ICAOHex_Domestic_Start': Type.String({ 
        description: 'ICAO HEX start value for domestic flights. E.g. A00000 for USA or C80000 for NZ.', 
        default: 'C80000'
    }),
    'ADSBX_ICAOHex_Domestic_End': Type.String({ 
        description: 'ICAO HEX start value for domestic flights. E.g. AFFFFF for USA or C87FFF for NZ.', 
        default: 'AFFFFF'
    }),
    'DEBUG': Type.Boolean({ 
        description: 'Print ADSBX results in logs.', 
        default: false })
});

/**
 * Schema for aircraft data returned by the ADSBExchange API
 * See API documentation: https://www.adsbexchange.com/version-2-api-wip/
 * 
 * This schema has been enhanced to properly validate all fields used in the code
 * to prevent potential runtime errors.
 */
// Define a minimal interface for aircraft data with just the fields we need
interface AircraftData {
    hex?: string;
    type?: string;
    flight?: string;
    r?: string;
    t?: string;
    alt_baro?: string | number;
    alt_geom?: string | number;
    gs?: number;
    track?: number;
    lat: number;
    lon: number;
    seen_pos: number;
    seen: number;
    category?: string;
    squawk?: string;
    emergency?: string;
    dbFlags?: number;
    group?: string;
    cot_type?: string;
    comments?: string;
}

// Define a more flexible schema for ADSBExchange API responses
const ADSBResponse = Type.Object({
    // Required fields
    hex: Type.String({
        description: 'ICAO 24-bit address (Mode S transponder code) in hex string format'
    }),
    type: Type.String({
        description: 'Type of ADS-B data source'
    }),
    lat: Type.Number({
        description: 'Latitude in decimal degrees'
    }),
    lon: Type.Number({
        description: 'Longitude in decimal degrees'
    }),
    seen_pos: Type.Number({
        description: 'How long ago (in seconds) the position was last reported'
    }),
    seen: Type.Number({
        description: 'How long ago (in seconds) a message was last received from this aircraft'
    }),
    
    // Optional fields with defaults or validation
    flight: Type.Optional(Type.String({
        description: 'Flight or callsign',
        default: ''
    })),
    r: Type.Optional(Type.String({
        description: 'Registration (tail number)',
        default: ''
    })),
    t: Type.Optional(Type.String({
        description: 'Aircraft type',
        default: ''
    })),
    dbFlags: Type.Optional(Type.Number({
        description: 'Database flags, bit 0 = military'
    })),
    alt_baro: Type.Optional(Type.Union([
        Type.Number({
            description: 'Barometric altitude in feet'
        }),
        Type.String({
            description: 'Barometric altitude as string (e.g. "ground")'
        })
    ])),
    alt_geom: Type.Optional(Type.Union([
        Type.Number({
            description: 'Geometric altitude in feet'
        }),
        Type.String({
            description: 'Geometric altitude as string'
        })
    ])),
    gs: Type.Optional(Type.Number({
        description: 'Ground speed in knots'
        // No default - will use NaN for unknown values
    })),
    track: Type.Optional(Type.Number({
        description: 'Track angle in degrees (0-359)',
        default: UNKNOWN_COURSE
    })),
    baro_rate: Type.Optional(Type.Number({
        description: 'Rate of climb/descent in feet per minute'
    })),
    geom_rate: Type.Optional(Type.Number({
        description: 'Geometric rate of climb/descent in feet per minute'
    })),
    squawk: Type.Optional(Type.String({
        description: 'Transponder code (octal)',
        default: ''
    })),
    emergency: Type.Optional(Type.String({
        description: 'Emergency status',
        default: 'none'
    })),
    category: Type.Optional(Type.String({
        description: 'Aircraft category (A0-A7, B0-B7, C0-C7, D0-D7)',
        default: ''
    })),
    nav_qnh: Type.Optional(Type.Number({
        description: 'Altimeter setting (QNH/QFE) in millibars'
    })),
    nav_altitude_mcp: Type.Optional(Type.Number({
        description: 'MCP/FCU selected altitude in feet'
    })),
    nav_heading: Type.Optional(Type.Number({
        description: 'Selected heading in degrees'
    })),
    nav_modes: Type.Optional(Type.Array(Type.String(), {
        description: 'Navigation modes active on the aircraft'
    })),
    ias: Type.Optional(Type.Number({
        description: 'Indicated airspeed in knots'
    })),
    tas: Type.Optional(Type.Number({
        description: 'True airspeed in knots'
    })),
    mach: Type.Optional(Type.Number({
        description: 'Mach number'
    })),
    wd: Type.Optional(Type.Number({
        description: 'Wind direction in degrees'
    })),
    ws: Type.Optional(Type.Number({
        description: 'Wind speed in knots'
    })),
    track_rate: Type.Optional(Type.Number({
        description: 'Rate of change of track in degrees/second'
    })),
    roll: Type.Optional(Type.Number({
        description: 'Roll angle in degrees'
    })),
    mag_heading: Type.Optional(Type.Number({
        description: 'Magnetic heading in degrees'
    })),
    true_heading: Type.Optional(Type.Number({
        description: 'True heading in degrees'
    })),
    nic: Type.Optional(Type.Number({
        description: 'Navigation Integrity Category'
    })),
    rc: Type.Optional(Type.Number({
        description: 'Radius of Containment in meters'
    })),
    version: Type.Optional(Type.Number({
        description: 'ADS-B version number'
    })),
    nic_baro: Type.Optional(Type.Number({
        description: 'Barometric altitude integrity code'
    })),
    nac_p: Type.Optional(Type.Number({
        description: 'Position accuracy category'
    })),
    nac_v: Type.Optional(Type.Number({
        description: 'Velocity accuracy category'
    })),
    sil: Type.Optional(Type.Number({
        description: 'Source Integrity Level'
    })),
    sil_type: Type.Optional(Type.String({
        description: 'Source Integrity Level type'
    })),
    gva: Type.Optional(Type.Number({
        description: 'Geometric Vertical Accuracy'
    })),
    sda: Type.Optional(Type.Number({
        description: 'System Design Assurance'
    })),
    alert: Type.Optional(Type.Number({
        description: 'Alert bit value'
    })),
    spi: Type.Optional(Type.Number({
        description: 'Special Position Identification bit value'
    })),
    mlat: Type.Optional(Type.Array(Type.Any(), {
        description: 'MLAT fields'
    })),
    tisb: Type.Optional(Type.Array(Type.Any(), {
        description: 'TIS-B fields'
    })),
    messages: Type.Optional(Type.Number({
        description: 'Number of messages received from this aircraft'
    })),
    rssi: Type.Optional(Type.Number({
        description: 'Received Signal Strength Indicator'
    })),
    dst: Type.Optional(Type.Number({
        description: 'Distance from receiver in nautical miles'
    })),
    
    // Fields added by our ETL process
    group: Type.Optional(Type.String({
        default: 'None',
        description: 'Provided by the join with ADSBX_Includes items'
    })),
    cot_type: Type.Optional(Type.String({
        default: 'None',
        description: 'Provided by the join with ADSBX_Includes items'
    })),
    comments: Type.Optional(Type.String({
        default: '',
        description: 'Provided by the join with ADSBX_Includes items'
    })),
})

/**
 * Main ETL task class for processing ADSBExchange data
 * Fetches aircraft data, filters and transforms it, and submits it to CloudTAK
 */
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

    /**
     * Main control function that executes the ETL process
     * 1. Fetches aircraft data from ADSBExchange API
     * 2. Processes and transforms the data
     * 3. Filters based on configuration
     * 4. Submits the data to CloudTAK
     */
    async control() {
        const env = await this.env(Env);

        const api = `${env.ADSBX_API}/v2/lat/${env['Query_LatLon'].split(',')[0].trim()}/lon/${env['Query_LatLon'].split(',')[1].trim()}/dist/${env['Query_Dist']}/`;

        const url = new URL(api);
        url.searchParams.append('apiKey', env.ADSBX_Token);
        url.searchParams.append('cacheBuster', String(new Date().getTime()));

        // Fetch aircraft data from ADSBExchange with error handling
        let body;
        try {
            const res = await fetch(url, {
                headers: {
                    'x-rapidapi-key': env.ADSBX_Token,
                    'api-auth': env.ADSBX_Token
                }
            });
            
            if (!res.ok) {
                throw new Error(`ADSBX API returned status ${res.status}: ${res.statusText}`);
            }
            
            // Get the raw response first
            const rawResponse = await res.json();
            
            if (env.DEBUG) {
                console.log('Raw API response:', JSON.stringify(rawResponse).substring(0, 1000) + '...');
            }
            
            // Use a simple approach that doesn't rely on strict validation
            // This is more resilient to API changes and variations
            if (rawResponse && typeof rawResponse === 'object' && 'ac' in rawResponse && Array.isArray(rawResponse.ac)) {
                // Use type assertion to help TypeScript understand the structure
                body = rawResponse as { ac: AircraftData[], msg: string };
                console.log(`Processing ${body.ac.length} aircraft`);
            } else {
                throw new Error('Invalid API response format: missing aircraft data');
            }
        } catch (error) {
            console.error(`Error fetching ADSBX data: ${error.message}`);
            // Return empty feature collection on error
            await this.submit({
                type: 'FeatureCollection',
                features: []
            });
            return;
        }

        // Create lookup maps for registrations and ICAO hex codes (for efficient matching)
        // These maps are used both for updating aircraft details and for filtering
        // Creating them once here avoids duplicating the code and improves performance
        const includesMap = new Map();
        const hexMap = new Map();
        for (const include of env.ADSBX_Includes) {
            // Add to registration map
            if (include.registration) {
                includesMap.set(include.registration.toLowerCase().trim(), include);
            }
            // Add to ICAO hex map
            if (include.ICAO_hex) {
                hexMap.set(include.ICAO_hex.toLowerCase().trim(), include);
            }
        }

        // Map to store processed aircraft data by ID (registration or flight number)
        const ids = new Map();

        // Process each aircraft from the API response
        for (const ac of body.ac) {
            if (!ac.flight && !ac.r) continue;

            if (env.ADSBX_Ignore_Tower_Vehicles && (ac.r == 'TWR' || ac.r == 'GND' || ac.type == 'adsb_icao_nt' )) continue; // Ignore tower, ground vehicles and test equipment

            const id = (ac.r || ac.flight).toLowerCase().trim();
            const coordinates = [ac.lon, ac.lat];

            // Handle altitude conversion from feet to meters with proper type checking
            if (ac.alt_geom !== undefined && ac.alt_geom !== null) {
                // Convert to number if it's a string, or use the number directly
                const altitudeValue = typeof ac.alt_geom === 'string' ? parseFloat(ac.alt_geom) : ac.alt_geom;
                
                // Only add valid numeric altitudes (including negative values for below sea level)
                if (!isNaN(altitudeValue)) {
                    coordinates.push(altitudeValue * FEET_TO_METERS);
                }
            } else {
                // Add NaN for unknown altitude per CoT specification
                coordinates.push(Number.NaN);
            }

            if (!id.trim().length) continue;

            // Determine the type of aircraft (fixed wing, rotorcraft, airship/balloon, etc.)
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

            // Helper function to safely compare ICAO hex codes
            // Converts hex strings to integers for proper numerical comparison
            function isHexInRange(hex: string, startHex: string, endHex: string): boolean {
                if (!hex) return false;
                
                // Normalize hex strings: remove whitespace, convert to uppercase, ensure 6 characters
                const normalizeHex = (h: string) => {
                    h = h.trim().toUpperCase();
                    // Pad with leading zeros if shorter than 6 characters
                    return h.padStart(6, '0');
                };
                
                const hexNorm = normalizeHex(hex);
                const startNorm = normalizeHex(startHex);
                const endNorm = normalizeHex(endHex);
                
                // Convert hex strings to integers for numerical comparison
                const hexVal = parseInt(hexNorm, 16);
                const startVal = parseInt(startNorm, 16);
                const endVal = parseInt(endNorm, 16);
                
                return hexVal >= startVal && hexVal <= endVal;
            }
            
            // Determine whether the aircraft is a domestic or foreign flight
            // Based on the ICAO Hex code, which is a 6-character alphanumeric code assigned to each aircraft
            // https://www.aerotransport.org/html/ICAO_hex_decode.html
            let ac_affiliation;
            if (ac.hex && isHexInRange(ac.hex, env.ADSBX_ICAOHex_Domestic_Start, env.ADSBX_ICAOHex_Domestic_End)) {
                ac_affiliation = '-f'; // Friendly (Local civilian)
            } else {
                ac_affiliation = '-n'; // Neutral (Foreign civilian)
            }

            // Determine whether the aircraft is civilian or military
            // https://www.adsbexchange.com/version-2-api-wip/
            let ac_civmil = '-C'; // Civilian
            if (ac.dbFlags !== undefined && ac.dbFlags % 2 !== 0) {
                ac_civmil = '-M'; // Military
                if (ac.hex && isHexInRange(ac.hex, env.ADSBX_ICAOHex_Domestic_Start, env.ADSBX_ICAOHex_Domestic_End)) {
                    ac_affiliation = '-f'; // Friendly (Local Military)
                } else {
                    ac_affiliation = '-u'; // Unknown (Foreign Military)
                }
            }

            // Determine whether the aircraft is in emergency mode
            // https://www.adsbexchange.com/version-2-api-wip/
            const isEmergency = ac.emergency !== undefined && ac.emergency !== 'none';
            
            // Check if this aircraft is in our includes list by ICAO hex first, then registration
            let include;
            
            // First try to find by ICAO hex
            if (ac.hex) {
                include = hexMap.get(ac.hex.toLowerCase().trim());
            }
            
            // If not found by ICAO hex, try to find by registration
            if (!include) {
                include = includesMap.get(id);
            }
            
            if (include) {
                if (include.group !== undefined) {
                    ac.group = include.group;
                }
                if (include.cot_type !== undefined) {
                    ac.cot_type = include.cot_type;
                }
                if (include.comments !== undefined) {
                    ac.comments = include.comments;
                }
            }

            // Define interface for feature properties with optional detail field
            interface FeatureProperties {
                type: string;
                callsign: string;
                time: Date;
                start: Date;
                speed: number;
                course: number;
                metadata: typeof ac;
                remarks: string;
                detail?: { alert: string };
                icon?: string;
            }
            
            // Helper function to build structured remarks
            function buildRemarks(aircraft: AircraftData): string {
                const remarksObj: Record<string, string> = {
                    'Flight': (aircraft.flight || 'Unknown').trim(),
                    'Registration': (aircraft.r || 'Unknown').trim(),
                    'Type': (aircraft.t || 'Unknown').trim(),
                    'Category': (aircraft.category || 'Unknown').trim(),
                    'Emergency': (aircraft.emergency || 'Unknown').trim(),
                    'Squawk': (aircraft.squawk || 'Unknown').trim()
                };
                
                // Add altitude information
                if (aircraft.alt_baro !== undefined) {
                    remarksObj['Alt Baro'] = typeof aircraft.alt_baro === 'number' ? 
                        `${aircraft.alt_baro} ft` : aircraft.alt_baro.toString();
                }
                
                if (aircraft.alt_geom !== undefined) {
                    remarksObj['Alt Geom'] = typeof aircraft.alt_geom === 'number' ? 
                        `${aircraft.alt_geom} ft` : aircraft.alt_geom.toString();
                }
                
                // Add optional fields if present
                if (aircraft.group && aircraft.group.trim() !== 'None' && aircraft.group.trim() !== 'UNKNOWN') {
                    remarksObj['Group'] = aircraft.group.replace(/_/g, '-').trim();
                }
                
                if (aircraft.comments) {
                    remarksObj['Comments'] = aircraft.comments.trim();
                }
                
                // Build remarks string with a specific order
                const orderedKeys = [
                    'Flight', 'Registration', 'Type', 'Category', 
                    'Alt Baro', 'Alt Geom', 'Emergency', 'Squawk', 'Group', 'Comments'
                ];
                
                return orderedKeys
                    .filter(key => key in remarksObj)
                    .map(key => `${key}: ${remarksObj[key]}`)
                    .join('\n');
            }
            
            // Prepare the feature properties
            const properties: FeatureProperties = {
                type: 'a' + ac_affiliation + '-A' + ac_civmil + ac_type,
                callsign: (ac.flight || '').trim(),
                time: new Date(Date.now() - (ac.seen_pos * 1000)),
                start: new Date(Date.now() - (ac.seen_pos * 1000)),
                speed: (typeof ac.gs === 'number' ? ac.gs * KNOTS_TO_MPS : Number.NaN), // Use NaN for unknown speed per CoT spec
                course: (typeof ac.track === 'number' ? ac.track : UNKNOWN_COURSE), // Use NaN for unknown course per CoT spec
                metadata: ac,
                remarks: buildRemarks(ac)
            };
            
            // Add alert attribute for emergency aircraft if configured
            if (isEmergency && env.ADSBX_Emergency_Alert) {
                properties.detail = {
                    alert: "red" // Use red alert level for emergency aircraft
                };
            }
            
            ids.set(id, {
                id: id,
                type: 'Feature',
                properties: properties,
                geometry: {
                    type: 'Point',
                    coordinates
                }
            });

            // If the aircraft has a group, set the icon based on the group from the 'Public Safety Air' icon set
            // https://tak.gov/public-safety-air-icons/
            // This is used to display different icons for different types of public safety aircraft    
            const feat = ids.get(id);
            if (ac.group && ac.group.trim() !== 'UNKNOWN' && ac.group.trim() !== 'None' && env.ADSBX_Use_Icon) {
                const groupName = ac.group.trim();
                // Validate that the group is in our list of valid icon groups before setting the icon
                if (VALID_ICON_GROUPS.has(groupName)) {
                    feat.properties.icon = PUBLIC_SAFETY_AIR_ICON_PATH + groupName + '.png';
                } else {
                    // Log a warning if an invalid group is encountered
                    console.warn(`Warning: Invalid icon group '${groupName}' for aircraft ${id}. Using default icon.`);
                }
            }

            // If the aircraft has a custom CoT type defined, use it
            if (ac.cot_type && ac.cot_type.trim() !== 'None' && ac.cot_type.trim() !== 'UNKNOWN') {
                feat.properties.type = ac.cot_type.trim();
            }
        }

        // Prepare array for the final feature collection
        const features = [];
        
        // Helper function to parse and rebuild remarks in a structured way
        function updateRemarks(remarks: string, updates: Record<string, string | null>): string {
            // Parse existing remarks into a structured object
            const lines = remarks.split('\n');
            const remarksObj: Record<string, string> = {};
            
            // Extract existing key-value pairs
            for (const line of lines) {
                const colonIndex = line.indexOf(':');
                if (colonIndex > 0) {
                    const key = line.substring(0, colonIndex).trim();
                    const value = line.substring(colonIndex + 1).trim();
                    remarksObj[key] = value;
                }
            }
            
            // Apply updates
            for (const [key, value] of Object.entries(updates)) {
                if (value === null) {
                    // Remove the field if value is null
                    delete remarksObj[key];
                } else {
                    // Update or add the field
                    remarksObj[key] = value;
                }
            }
            
            // Rebuild remarks string with a specific order
            const orderedKeys = [
                'Flight', 'Registration', 'Type', 'Category', 
                'Alt Baro', 'Alt Geom', 'Emergency', 'Squawk', 'Group', 'Comments'
            ];
            
            // Start with ordered keys
            const result = orderedKeys
                .filter(key => key in remarksObj)
                .map(key => `${key}: ${remarksObj[key]}`);
            
            // Add any remaining keys not in the ordered list
            for (const [key, value] of Object.entries(remarksObj)) {
                if (!orderedKeys.includes(key)) {
                    result.push(`${key}: ${value}`);
                }
            }
            
            return result.join('\n');
        }

        // Apply filtering based on configuration
        if (env.ADSBX_Filtering) {
            // Create a set to track processed aircraft IDs
            const processedIds = new Set();
            
            // Process aircraft that match by ICAO hex first
            for (const [hexCode, include] of hexMap.entries()) {
                // Find aircraft with matching hex code
                for (const [id, feat] of ids.entries()) {
                    if (processedIds.has(id)) continue; // Skip already processed
                    
                    const ac = feat.properties.metadata;
                    if (ac.hex && ac.hex.toLowerCase().trim() === hexCode) {
                        // Apply customizations
                        if (include.group !== undefined) {
                            feat.properties.metadata.group = include.group;
                            
                            // Update remarks with the new group
                            if (include.group.trim() !== 'None' && include.group.trim() !== 'UNKNOWN') {
                                feat.properties.remarks = updateRemarks(
                                    feat.properties.remarks, 
                                    { 'Group': include.group.replace(/_/g, '-').trim() }
                                );
                            } else {
                                // Remove Group field if None/UNKNOWN
                                feat.properties.remarks = updateRemarks(
                                    feat.properties.remarks, 
                                    { 'Group': null }
                                );
                            }
                        }

                        if (include.comments !== undefined) {
                            feat.properties.metadata.comments = include.comments;
                            
                            // Update remarks with the new comments
                            if (include.comments) {
                                feat.properties.remarks = updateRemarks(
                                    feat.properties.remarks, 
                                    { 'Comments': include.comments.trim() }
                                );
                            } else {
                                // Remove Comments field if empty
                                feat.properties.remarks = updateRemarks(
                                    feat.properties.remarks, 
                                    { 'Comments': null }
                                );
                            }
                        }
                        
                        processedIds.add(id);
                        features.push(feat);
                    }
                }
            }
            
            // Process aircraft that match by registration
            for (const [id, include] of includesMap.entries()) {
                if (ids.has(id) && !processedIds.has(id)) {
                    const feat = ids.get(id);

                    if (include && include.ICAO_hex) {
                        // No need to update callsign here as we're now matching on hex code
                    }

                    // Update metadata and remarks for group
                    if (include && include.group) {
                        feat.properties.metadata.group = include.group;
                        
                        // Update remarks with the new group
                        if (include.group.trim() !== 'None' && include.group.trim() !== 'UNKNOWN') {
                            feat.properties.remarks = updateRemarks(
                                feat.properties.remarks, 
                                { 'Group': include.group.replace(/_/g, '-').trim() }
                            );
                        } else {
                            // Remove Group field if None/UNKNOWN
                            feat.properties.remarks = updateRemarks(
                                feat.properties.remarks, 
                                { 'Group': null }
                            );
                        }
                    }

                    // Update metadata and remarks for comments
                    if (include && include.comments !== undefined) {
                        feat.properties.metadata.comments = include.comments;
                        
                        // Update remarks with the new comments
                        if (include.comments) {
                            feat.properties.remarks = updateRemarks(
                                feat.properties.remarks, 
                                { 'Comments': include.comments.trim() }
                            );
                        } else {
                            // Remove Comments field if empty
                            feat.properties.remarks = updateRemarks(
                                feat.properties.remarks, 
                                { 'Comments': null }
                            );
                        }
                    }

                    // Add to features array and mark as processed
                    processedIds.add(id);
                    features.push(feat);
                }
            }
        } else {
            // When filtering is disabled, include all aircraft
            // Simply add all values from the ids Map to the features array
            for (const feat of ids.values()) {
                features.push(feat);
            }
        }

        console.log(`ok - fetched ${ids.size} aircraft`);
        
        // Create the final GeoJSON feature collection to submit
        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features
        };

        await this.submit(fc);
    }
}

// For local development testing
await local(new Task(import.meta.url), import.meta.url);

// AWS Lambda handler function
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

