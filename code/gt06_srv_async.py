#!/bin/python

"""
Async TCP Server for GT06 protocol using asyncio streams.
Based on gt06_server.py but using asyncio instead of threading.

This server implements the protocol documented by the chinese
company TOPIN to handle communication with their GPS trackers,
sending and receiving TCP packets.

This program will create a TCP socket and each client will have
its dedicated async task, so that multiple clients can connect 
simultaneously.
"""

from dotenv import load_dotenv
import asyncio
from datetime import datetime
from dateutil import tz
import requests
import math
import os


# Declare common Hex codes for packets
hex_dict = {
    'start': '78', 
    'stop_1': '0D', 
    'stop_2': '0A'
}

protocol_dict = {
    'protocol': {
        '01': 'login',
        '05': 'supervision',
        '08': 'heartbeat', 
        '10': 'gps_positioning', 
        '11': 'gps_offline_positioning', 
        '12': 'gps_reporting',  # T12: GPS + LBS reporting
        '13': 'status', 
        '14': 'hibernation', 
        '15': 'reset', 
        '16': 'gps_status_reporting',  # T16: GPS + LBS + device status
        '17': 'wifi_offline_positioning', 
        '30': 'time', 
        '43': 'mom_phone_WTFISDIS?', 
        '56': 'stop_alarm', 
        '57': 'setup', 
        '58': 'synchronous_whitelist', 
        '67': 'restore_password', 
        '69': 'wifi_positioning', 
        '80': 'manual_positioning', 
        '81': 'battery_charge', 
        '82': 'charger_connected', 
        '83': 'charger_disconnected', 
        '94': 'vibration_received', 
        '98': 'position_upload_interval'
    }, 
    'response_method': {
        'login': 'login',
        'logout': 'logout', 
        'supervision': '',
        'heartbeat': '', 
        'gps_positioning': 'datetime_response', 
        'gps_offline_positioning': 'datetime_response', 
        'gps_reporting': 'datetime_response', 
        'status': '', 
        'hibernation': '', 
        'reset': '', 
        'gps_status_reporting': 'datetime_response', 
        'wifi_offline_positioning': 'datetime_response', 
        'time': 'time_response', 
        'stop_alarm': '', 
        'setup': 'setup', 
        'synchronous_whitelist': '', 
        'restore_password': '', 
        'wifi_positioning': 'datetime_position_response', 
        'manual_positioning': '', 
        'battery_charge': '', 
        'charger_connected': '', 
        'charger_disconnected': '', 
        'vibration_received': '', 
        'position_upload_interval': 'upload_interval_response'
    }
}

# Store client data into dictionaries
addresses = {}
positions = {}


async def LOGGER(event, filename, ip, client, type, data):
    """
    A logging function to store all input packets, 
    as well as output ones when they are generated.

    There are two types of logs implemented: 
        - a general (info) logger that will keep track of all 
            incoming and outgoing packets,
        - a position (location) logger that will write to a 
            file contianing only results og GPS or LBS data.
    """
    
    log_dir = './logs/'
    os.makedirs(log_dir, exist_ok=True)
    
    with open(os.path.join(log_dir, filename), 'a+') as log:
        if (event == 'info'):
            # TSV format of: Timestamp, Client IP, IN/OUT, Packet
            logMessage = datetime.now().strftime('%Y/%m/%d %H:%M:%S') + '\t' + ip + '\t' + client + '\t' + type + '\t' + data + '\n'
        elif (event == 'location'):
            # TSV format of: Timestamp, Client IP, Location DateTime, GPS/LBS, Validity, Nb Sat, Latitude, Longitude, Accuracy, Speed, Heading
            logMessage = datetime.now().strftime('%Y/%m/%d %H:%M:%S') + '\t' + ip + '\t' + client + '\t' + '\t'.join(list(str(x) for x in data.values())) + '\n'
        log.write(logMessage)


async def handle_client(reader, writer):
    """
    Takes reader and writer streams as arguments. 
    Handles a single client connection, by listening indefinitely for packets.
    """
    
    client_address = writer.get_extra_info('peername')
    client_id = id(writer)
    
    print(f'{client_address[0]}:{client_address[1]} has connected.')
    
    # Initialize the dictionaries
    addresses[client_id] = {}
    positions[client_id] = {}
    addresses[client_id]['address'] = client_address
    addresses[client_id]['reader'] = reader
    addresses[client_id]['writer'] = writer
    
    # Initialize dictionaries for that client
    positions[client_id]['wifi'] = []
    positions[client_id]['gsm-cells'] = []
    positions[client_id]['gsm-carrier'] = {}
    positions[client_id]['gps'] = {}

    # Keep receiving and analyzing packets until end of time
    # or until device sends disconnection signal
    keepAlive = True
    buffer = b""
    
    try:
        while keepAlive:
            try:
                # Read data from stream
                data = await asyncio.wait_for(reader.read(4096), timeout=300.0)
                
                # If no data received, connection is closed
                if not data:
                    print(f'[{client_address[0]}] DISCONNECTED: connection was closed by client.')
                    break
                
                # Add to buffer
                buffer += data
                
                # Process complete packets from buffer
                while True:
                    # Look for packet start (0x78 0x78)
                    start_idx = buffer.find(b'\x78\x78')
                    if start_idx == -1:
                        # No start marker found, keep waiting
                        break
                    
                    # Remove any data before the start marker
                    if start_idx > 0:
                        buffer = buffer[start_idx:]
                    
                    # Look for packet end (0x0D 0x0A)
                    end_idx = buffer.find(b'\x0D\x0A', 2)
                    if end_idx == -1:
                        # End marker not found yet, need more data
                        break
                    
                    # Extract complete packet
                    packet = buffer[:end_idx + 2]
                    buffer = buffer[end_idx + 2:]
                    
                    # Process the packet
                    if len(packet) > 0:
                        print(f'[{client_address[0]}] IN Hex : {packet.hex()} (length in bytes = {len(packet)})')
                        keepAlive = await read_incoming_packet(client_id, packet)
                        await LOGGER('info', 'server_log.txt', client_address[0], addresses[client_id].get('imei', 'unknown'), 'IN', packet.hex())
                        
                        # Disconnect if client sent disconnect signal
                        if not keepAlive:
                            print(f'[{client_address[0]}] DISCONNECTED: socket was closed by client.')
                            break
                
            except asyncio.TimeoutError:
                print(f'[{client_address[0]}] Timeout: no data received for 5 minutes.')
                break
            except Exception as e:
                print(f'[{client_address[0]}] ERROR: socket was closed due to the following exception:')
                print(e)
                break
                
    finally:
        # Clean up
        writer.close()
        await writer.wait_closed()
        if client_id in addresses:
            del addresses[client_id]
        if client_id in positions:
            del positions[client_id]
        print(f"Connection to {client_address[0]}:{client_address[1]} closed.")


async def read_incoming_packet(client_id, packet):
    """
    Handle incoming packets to identify the protocol they are related to,
    and then redirects to response functions that will generate the apropriate 
    packet that should be sent back.
    Actual sending of the response packet will be done by an external function.
    """

    # Convert hex string into list for convenience
    # Strip packet of bits 1 and 2 (start 0x78 0x78) and n-1 and n (end 0x0d 0x0a)
    packet_list = [packet.hex()[i:i+2] for i in range(4, len(packet.hex())-4, 2)]
    
    # DEBUG: Print the role of current packet
    protocol_hex = packet_list[1] if len(packet_list) > 1 else '00'
    protocol_name = protocol_dict['protocol'].get(protocol_hex, 'unknown')
    protocol_method = protocol_dict['response_method'].get(protocol_name, '')
    print(f'The current packet is for protocol: {protocol_name} which has method: {protocol_method}')
    
    # Prepare the response, initialize as empty
    r = ''

    # Get the protocol name and react accordingly
    if (protocol_name == 'login'):
        r = await answer_login(client_id, packet_list)
    
    elif (protocol_name == 'gps_positioning' or protocol_name == 'gps_offline_positioning'):
        r = await answer_gps(client_id, packet_list)

    elif (protocol_name == 'gps_reporting' or protocol_name == 'gps_status_reporting'):
        r = await answer_gps_reporting(client_id, packet_list, protocol_name)

    elif (protocol_name == 'status'):
        # Status can sometimes carry signal strength and sometimes not
        if (len(packet_list) > 0 and packet_list[0] == '06'): 
            print(f'[{addresses[client_id]["address"][0]}] STATUS : Battery = {int(packet_list[2], base=16)}; Sw v. = {int(packet_list[3], base=16)}; Status upload interval = {int(packet_list[4], base=16)}')
        elif (len(packet_list) > 0 and packet_list[0] == '07'): 
            print(f'[{addresses[client_id]["address"][0]}] STATUS : Battery = {int(packet_list[2], base=16)}; Sw v. = {int(packet_list[3], base=16)}; Status upload interval = {int(packet_list[4], base=16)}; Signal strength = {int(packet_list[5], base=16)}')
        # Exit function without altering anything
        return(True)
    
    elif (protocol_name == 'hibernation'):
        # Exit function returning False to break main while loop in handle_client()
        print(f'[{addresses[client_id]["address"][0]}] STATUS : Sent hibernation packet. Disconnecting now.')
        return(False)

    elif (protocol_name == 'setup'):
        # TODO: HANDLE NON-DEFAULT VALUES
        r = answer_setup(packet_list, '0300', '00110001', '000000', '000000', '000000', '00', '000000', '000000', '000000', '00', '0000', '0000', ['', '', ''])

    elif (protocol_name == 'time'):
        r = answer_time(packet_list)

    elif (protocol_name == 'wifi_positioning' or protocol_name == 'wifi_offline_positioning'):
        r = await answer_wifi_lbs(client_id, packet_list)

    elif (protocol_name == 'position_upload_interval'):
        r = answer_upload_interval(client_id, packet_list)
    
    # Send response to client, if it exists
    if (r != ''):
        print(f'[{addresses[client_id]["address"][0]}] OUT Hex : {r} (length in bytes = {len(bytes.fromhex(r))})')
        await send_response(client_id, r)
    
    # Return True to avoid failing in main while loop in handle_client()
    return(True)


async def answer_login(client_id, query):
    """
    This function extracts IMEI and Software Version from the login packet. 
    The IMEI and Software Version will be stored into a client dictionary to 
    allow handling of multiple devices at once, in the future.
    
    The client_id is passed as an argument because it is in this packet
    that IMEI is sent and will be stored in the address dictionary.
    """
    
    # Read data: Bits 2 through 9 are IMEI and 10 is software version
    if len(query) < 11:
        return ''
    
    protocol = query[1]
    addresses[client_id]['imei'] = ''.join(query[2:10])[1:]
    addresses[client_id]['software_version'] = int(query[10], base=16)

    # DEBUG: Print IMEI and software version
    print(f"Detected IMEI : {addresses[client_id]['imei']} and Sw v. : {addresses[client_id]['software_version']}")

    # Prepare response: in absence of control values, 
    # always accept the client
    response = '01'
    r = generic_response(response)
    return(r)


def answer_setup(query, uploadIntervalSeconds, binarySwitch, alarm1, alarm2, alarm3, dndTimeSwitch, dndTime1, dndTime2, dndTime3, gpsTimeSwitch, gpsTimeStart, gpsTimeStop, phoneNumbers):
    """
    Synchronous setup is initiated by the device who asks the server for 
    instructions.
    These instructions will consists of bits for different flags as well as
    alarm clocks ans emergency phone numbers.
    """
    
    # Read protocol
    if len(query) < 2:
        return ''
    protocol = query[1]

    # Convert binarySwitch from byte to hex
    binarySwitch = format(int(binarySwitch, base=2), '02X')

    # Convert phone numbers to 'ASCII' (?) by padding each digit with 3's and concatenate
    for n in range(len(phoneNumbers)):
        phoneNumbers[n] = bytes(phoneNumbers[n], 'UTF-8').hex()
    phoneNumbers = '3B'.join(phoneNumbers)

    # Build response
    response = uploadIntervalSeconds + binarySwitch + alarm1 + alarm2 + alarm3 + dndTimeSwitch + dndTime1 + dndTime2 + dndTime3 + gpsTimeSwitch + gpsTimeStart + gpsTimeStop + phoneNumbers
    r = make_content_response(hex_dict['start'] + hex_dict['start'], protocol, response, hex_dict['stop_1'] + hex_dict['stop_2'], ignoreDatetimeLength=False, ignoreSeparatorLength=False)
    return(r)


def answer_time(query):
    """
    Time synchronization is initiated by the device, which expects a response
    contianing current datetime over 7 bytes: YY YY MM DD HH MM SS.
    This function is a wrapper to generate the proper response
    """
    
    # Read protocol
    if len(query) < 2:
        return ''
    protocol = query[1]

    # Get current date and time into the pretty-fied hex format
    response = get_hexified_datetime(truncatedYear=False)

    # Build response
    r = make_content_response(hex_dict['start'] + hex_dict['start'], protocol, response, hex_dict['stop_1'] + hex_dict['stop_2'], ignoreDatetimeLength=False, ignoreSeparatorLength=False)
    return(r)


async def answer_gps(client_id, query):
    """
    GPS positioning can come into two packets that have the exact same structure, 
    but protocol can be 0x10 (GPS positioning) or 0x11 (Offline GPS positioning)... ?
    Anyway: the structure of these packets is constant, not like GSM or WiFi packets
    """

    # Reset positions lists (Wi-Fi, and LBS) and dictionary (carrier) for that client
    positions[client_id]['gps'] = {}

    # Read protocol
    if len(query) < 20:
        return ''
    protocol = query[1]

    # Extract datetime from incoming query to put into the response
    # Datetime is in HEX format here, contrary to LBS packets...
    # That means it's read as HEX(YY) HEX(MM) HEX(DD) HEX(HH) HEX(MM) HEX(SS)...
    dt = ''.join([ format(int(x, base = 16), '02d') for x in query[2:8] ])
    # GPS DateTime is at UTC timezone: we need to store that information in the object
    if (dt != '000000000000'): 
        dt = datetime.strptime(dt, '%y%m%d%H%M%S').replace(tzinfo=tz.tzutc())
    else:
        dt = None

    
    # Read in the incoming GPS positioning
    # Byte 8 contains length of packet on 1st char and number of satellites on 2nd char
    gps_data_length = int(query[8][0], base=16)
    gps_nb_sat = int(query[8][1], base=16)
    # Latitude and longitude are both on 4 bytes, and were multiplied by 30000
    # after being converted to seconds-of-angle. Let's convert them back to degree
    gps_latitude = int(''.join(query[9:13]), base=16) / (30000 * 60)
    gps_longitude = int(''.join(query[13:17]), base=16) / (30000 * 60)
    # Speed is on the next byte
    gps_speed = int(query[17], base=16)
    # Last two bytes contain flags in binary that will be interpreted
    gps_flags = format(int(''.join(query[18:20]), base=16), '0>16b')
    position_is_valid = gps_flags[3]
    # Flip sign of GPS latitude if South, longitude if West
    if (gps_flags[4] == '1'):
        gps_longitude = -gps_longitude
    if (gps_flags[5] == '0'):
        gps_latitude = -gps_latitude
    gps_heading = int(''.join(gps_flags[6:]), base = 2)

    # Store GPS information into the position dictionary and print them
    positions[client_id]['gps']['method'] = 'GPS'
    # In some cases dt is empty with value '000000000000': let's avoid that because it'll crash strptime
    if dt:
        positions[client_id]['gps']['datetime'] = dt.astimezone(tz.tzlocal()).strftime('%Y/%m/%d %H:%M:%S')
    else:
        positions[client_id]['gps']['datetime'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    # Special value for 'valid' flag when dt is '000000000000' which may be an invalid position after all
    positions[client_id]['gps']['valid'] = (2 if (dt is None and position_is_valid == '1') else int(position_is_valid))
    positions[client_id]['gps']['nb_sat'] = gps_nb_sat
    positions[client_id]['gps']['latitude'] = gps_latitude
    positions[client_id]['gps']['longitude'] = gps_longitude
    positions[client_id]['gps']['accuracy'] = 0.0
    positions[client_id]['gps']['speed'] = gps_speed
    positions[client_id]['gps']['heading'] = gps_heading
    print(f'[{addresses[client_id]["address"][0]}] POSITION/GPS : Valid = {position_is_valid}; Nb Sat = {gps_nb_sat}; Lat = {gps_latitude}; Long = {gps_longitude}; Speed = {gps_speed}; Heading = {gps_heading}')
    await LOGGER('location', 'location_log.txt', addresses[client_id]['address'][0], addresses[client_id].get('imei', 'unknown'), '', positions[client_id]['gps'])

    # Get current datetime for answering
    # TEST: Return datetime that was extracted from packet instead of current server datetime
    # response = get_hexified_datetime(truncatedYear=True)
    if dt:
        response = ''.join(query[2:8])
    else:
        response = get_hexified_datetime(truncatedYear=True)

    r = make_content_response(hex_dict['start'] + hex_dict['start'], protocol, response, hex_dict['stop_1'] + hex_dict['stop_2'], ignoreDatetimeLength=False, ignoreSeparatorLength=False, forceLengthToValue=0)
    return(r)


async def answer_gps_reporting(client_id, query, protocol_name):
    """
    Handle GPS reporting from T12 (GPS + LBS) or T16 (GPS + LBS + device status).
    This format is used by gt06_msg.py client library.
    
    Format from gt06_msg.py:
    - GPS: date_time(6 bytes BCD) + gps_len("c") + satellite_num(1) + lat(4) + lon(4) + speed(1) + status_course(2)
    - LBS: mcc(2) + mnc(1) + lac(2) + cell_id(3) [for T12]
    - For T16: LBS length(1) + LBS data + device_status
    """
    
    # Reset positions for that client
    positions[client_id]['gps'] = {}
    positions[client_id]['gsm-cells'] = []
    positions[client_id]['gsm-carrier'] = {}
    
    # Content starts at index 2 (after length and protocol)
    if len(query) < 3:
        return ''
    
    content_start = 2
    protocol = query[1]
    
    # Parse GPS data (19 bytes = 38 hex chars)
    # date_time: 6 bytes (12 hex chars) - BCD encoded
    # gps_len: 1 byte (2 hex chars) - should be "c" (0x0C)
    # satellite_num: 1 byte (2 hex chars)
    # latitude: 4 bytes (8 hex chars)
    # longitude: 4 bytes (8 hex chars)
    # speed: 1 byte (2 hex chars)
    # status_course: 2 bytes (4 hex chars)
    
    if len(query) < content_start + 19:
        return ''
    
    # Extract datetime (BCD encoded, 6 bytes = 12 hex chars)
    # Each byte contains two BCD digits (e.g., 0x22 = decimal 22)
    dt_hex = ''.join(query[content_start:content_start + 6])
    # Convert each hex byte to two decimal digits
    dt = ''.join([format(int(dt_hex[i:i+2], 16), '02d') for i in range(0, 12, 2)])
    if dt != '000000000000':
        try:
            dt = datetime.strptime(dt, '%y%m%d%H%M%S').replace(tzinfo=tz.tzutc())
        except ValueError:
            dt = None
    else:
        dt = None
    
    # Skip gps_len byte (should be "c")
    gps_len_idx = content_start + 6
    
    # Satellite number
    gps_nb_sat = int(query[gps_len_idx + 1], base=16)
    
    # Latitude (4 bytes = 8 hex chars) - multiplied by 6 * 3 * 10^5
    lat_start = gps_len_idx + 2
    gps_latitude = int(''.join(query[lat_start:lat_start + 8]), base=16) / (6 * 3 * 10 ** 5)
    
    # Longitude (4 bytes = 8 hex chars)
    lon_start = lat_start + 8
    gps_longitude = int(''.join(query[lon_start:lon_start + 8]), base=16) / (6 * 3 * 10 ** 5)
    
    # Speed (1 byte)
    speed_idx = lon_start + 8
    gps_speed = int(query[speed_idx], base=16)
    
    # Status and course (2 bytes = 4 hex chars)
    status_idx = speed_idx + 1
    status_course_hex = ''.join(query[status_idx:status_idx + 4])
    status_course_bits = format(int(status_course_hex, 16), '0>16b')
    
    # Parse status bits: is_real_time(1) + gps_onoff(1) + lon_ew(1) + lat_ns(1) + course(10)
    is_real_time = int(status_course_bits[0])
    gps_onoff = int(status_course_bits[1])
    lon_ew = int(status_course_bits[2])
    lat_ns = int(status_course_bits[3])
    gps_heading = int(status_course_bits[4:], base=2)
    
    # Apply direction signs
    # lat_ns: 0=South, 1=North
    # lon_ew: 0=East, 1=West
    if lat_ns == 0:  # South
        gps_latitude = -gps_latitude
    if lon_ew == 1:  # West
        gps_longitude = -gps_longitude
    
    position_is_valid = '1' if gps_onoff == 1 else '0'
    
    # Store GPS information
    positions[client_id]['gps']['method'] = 'GPS'
    if dt:
        positions[client_id]['gps']['datetime'] = dt.astimezone(tz.tzlocal()).strftime('%Y/%m/%d %H:%M:%S')
    else:
        positions[client_id]['gps']['datetime'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    positions[client_id]['gps']['valid'] = int(position_is_valid)
    positions[client_id]['gps']['nb_sat'] = gps_nb_sat
    positions[client_id]['gps']['latitude'] = gps_latitude
    positions[client_id]['gps']['longitude'] = gps_longitude
    positions[client_id]['gps']['accuracy'] = 0.0
    positions[client_id]['gps']['speed'] = gps_speed
    positions[client_id]['gps']['heading'] = gps_heading
    
    print(f'[{addresses[client_id]["address"][0]}] POSITION/GPS (T12/T16) : Valid = {position_is_valid}; Nb Sat = {gps_nb_sat}; Lat = {gps_latitude}; Long = {gps_longitude}; Speed = {gps_speed}; Heading = {gps_heading}')
    await LOGGER('location', 'location_log.txt', addresses[client_id]['address'][0], addresses[client_id].get('imei', 'unknown'), '', positions[client_id]['gps'])
    
    # Parse LBS data (if present)
    lbs_start = status_idx + 4
    if protocol_name == 'gps_status_reporting' and len(query) > lbs_start:
        # T16 format: LBS length byte, then LBS data, then device status
        lbs_len = int(query[lbs_start], base=16)
        lbs_data_start = lbs_start + 1
        
        if len(query) >= lbs_data_start + lbs_len * 2:
            # LBS: mcc(2 bytes) + mnc(1 byte) + lac(2 bytes) + cell_id(3 bytes)
            if lbs_len >= 4:
                mcc = int(''.join(query[lbs_data_start:lbs_data_start + 4]), base=16)
                mnc = int(query[lbs_data_start + 4], base=16)
                lac = int(''.join(query[lbs_data_start + 5:lbs_data_start + 9]), base=16)
                cell_id = int(''.join(query[lbs_data_start + 9:lbs_data_start + 15]), base=16)
                
                positions[client_id]['gsm-carrier']['MCC'] = mcc
                positions[client_id]['gsm-carrier']['MNC'] = mnc
                positions[client_id]['gsm-carrier']['n_gsm_cells'] = 1
                positions[client_id]['gsm-cells'].append({
                    'locationAreaCode': lac,
                    'cellId': cell_id,
                    'signalStrength': -100  # Not provided in T16
                })
    elif protocol_name == 'gps_reporting' and len(query) > lbs_start:
        # T12 format: LBS data directly follows GPS
        # LBS: mcc(2 bytes) + mnc(1 byte) + lac(2 bytes) + cell_id(3 bytes) = 8 bytes = 16 hex chars
        if len(query) >= lbs_start + 16:
            mcc = int(''.join(query[lbs_start:lbs_start + 4]), base=16)
            mnc = int(query[lbs_start + 4], base=16)
            lac = int(''.join(query[lbs_start + 5:lbs_start + 9]), base=16)
            cell_id = int(''.join(query[lbs_start + 9:lbs_start + 15]), base=16)
            
            positions[client_id]['gsm-carrier']['MCC'] = mcc
            positions[client_id]['gsm-carrier']['MNC'] = mnc
            positions[client_id]['gsm-carrier']['n_gsm_cells'] = 1
            positions[client_id]['gsm-cells'].append({
                'locationAreaCode': lac,
                'cellId': cell_id,
                'signalStrength': -100  # Not provided in T12
            })
    
    # Prepare response - return datetime from GPS data
    if dt:
        # Convert back to hex format for response
        dt_str = dt.strftime('%y%m%d%H%M%S')
        response = ''.join([format(int(dt_str[i:i+2]), '02X') for i in range(0, 12, 2)])
    else:
        response = get_hexified_datetime(truncatedYear=True)
    
    r = make_content_response(hex_dict['start'] + hex_dict['start'], protocol, response, hex_dict['stop_1'] + hex_dict['stop_2'], ignoreDatetimeLength=False, ignoreSeparatorLength=False, forceLengthToValue=0)
    return r


async def answer_wifi_lbs(client_id, query):
    """
    WiFi + LBS data can come into two packets that have the exact same structure, 
    but protocol can be 0x17 or 0x69. Likely similar to GPS/offline GPS... ?
    According to documentation 0x17 is an "offline" (cached?) query, which may be
    preserved and queried again until the right answer is returned.
    0x17 expects only datetime as an answer
    0x69 expects datetime, followed by a decoded position

    Packet structure is variable and consist in N WiFi hotspots (3 <= N <= 8) and
    N (2 <= N <= ?) GSM towers.

    WiFi hotspots are identified by BSSID (mac address; 6 bytes) and RSSI (1 byte).

    GSM is first defined as MCCMNC (2+1 bytes) and nearby towers are then 
    identified by LAC (2 bytes), Cell ID (2 bytes) and MCISS (1 byte).

    This function will not return anything but write to a dictionary that is accessible
    outside of the function. This is because WiFi/LBS packets expect two responses :
    - hexified datetime
    - decoded positions as latitude and longitude, based from transmitted elements.
    """

    # Reset positions lists (Wi-Fi, and LBS) and dictionary (carrier) for that client
    positions[client_id]['wifi'] = []
    positions[client_id]['gsm-cells'] = []
    positions[client_id]['gsm-carrier'] = {}
    positions[client_id]['gps'] = {}

    # Read protocol
    if len(query) < 8:
        return ''
    protocol = query[1]

    # Datetime is BCD-encoded in bytes 2:7, meaning it's read *directly* as YY MM DD HH MM SS
    # and does not need to be decoded from hex. YY value above 2000.
    dt = ''.join(query[2:8])
    # WiFi DateTime seems to be UTC timezone: add that info to the object
    if (dt != '000000000000'): 
        dt = datetime.strptime(dt, '%y%m%d%H%M%S').replace(tzinfo=tz.tzutc())
    else:
        dt = None

    # WIFI
    n_wifi = int(query[0])
    if (n_wifi > 0 and len(query) >= 8 + (7 * n_wifi)):
        for i in range(n_wifi):
            current_wifi = {'macAddress': ':'.join(query[(8 + (7 * i)):(8 + (7 * (i + 1)) - 2 + 1)]), # That +1 is because l[start:stop] returnes elements from start to stop-1...
                            'signalStrength': -int(query[(8 + (7 * (i + 1)) - 1)], base = 16)}
            positions[client_id]['wifi'].append(current_wifi)
            
            # Print Wi-Fi hotspots into the logs
            print(f'[{addresses[client_id]["address"][0]}] POSITION/WIFI : BSSID = {current_wifi["macAddress"]}; RSSI = {current_wifi["signalStrength"]}')

    # GSM Cell towers
    wifi_offset = 8 + (7 * n_wifi)
    if len(query) > wifi_offset:
        n_gsm_cells = int(query[wifi_offset])
        # The first three bytes after n_lbs are MCC(2 bytes)+MNC(1 byte)
        if len(query) > wifi_offset + 3:
            gsm_mcc = int(''.join(query[(wifi_offset + 1):(wifi_offset + 2 + 1)]), base=16)
            gsm_mnc = int(query[(wifi_offset + 3)], base=16)
            positions[client_id]['gsm-carrier']['n_gsm_cells'] = n_gsm_cells
            positions[client_id]['gsm-carrier']['MCC'] = gsm_mcc
            positions[client_id]['gsm-carrier']['MNC'] = gsm_mnc

            if (n_gsm_cells > 0 and len(query) >= wifi_offset + 4 + (5 * n_gsm_cells)):
                for i in range(n_gsm_cells):
                    current_gsm_cell = {'locationAreaCode': int(''.join(query[((wifi_offset + 4) + (5 * i)):((wifi_offset + 4) + (5 * i) + 1 + 1)]), base=16),
                                        'cellId': int(''.join(query[((wifi_offset + 4) + (5 * i) + 1 + 1):((wifi_offset + 4) + (5 * i) + 2 + 1 + 1)]), base=16),
                                        'signalStrength': -int(query[((wifi_offset + 4) + (5 * i) + 2 + 1 + 1)], base=16)}
                    positions[client_id]['gsm-cells'].append(current_gsm_cell)
                    
                    # Print LBS data into logs as well
                    print(f'[{addresses[client_id]["address"][0]}] POSITION/LBS : LAC = {current_gsm_cell["locationAreaCode"]}; CellID = {current_gsm_cell["cellId"]}; MCISS = {current_gsm_cell["signalStrength"]}')

    # Build first stage of response with dt and send it to devices
    if dt:
        dt_str = dt.strftime('%y%m%d%H%M%S')
    else:
        dt_str = datetime.now().strftime('%y%m%d%H%M%S')
    r_1 = make_content_response(hex_dict['start'] + hex_dict['start'], protocol, dt_str, hex_dict['stop_1'] + hex_dict['stop_2'], ignoreDatetimeLength=False, ignoreSeparatorLength=False, forceLengthToValue=0)

    # Build second stage of response, which requires decoding the positioning data
    print("Decoding location-based data using OpenCellID API...")
    decoded_position = await OpenCellID_geolocation_service(opencellid_api_key, positions[client_id])
    
    # Handle errors in decoding location
    if (list(decoded_position.keys())[0] == 'error'):
        # OpenCellID API returned an error
        positions[client_id]['gps']['method'] = 'LBS'
        positions[client_id]['gps']['datetime'] = ''
        positions[client_id]['gps']['valid'] = 0
        positions[client_id]['gps']['nb_sat'] = ''
        positions[client_id]['gps']['latitude'] = ''
        positions[client_id]['gps']['longitude'] = ''
        positions[client_id]['gps']['accuracy'] = ''
        positions[client_id]['gps']['speed'] = ''
        positions[client_id]['gps']['heading'] = ''
    
    else:
        # OpenCellID API returned a location
        if (len(positions[client_id]['wifi']) > 0):
            positions[client_id]['gps']['method'] = 'LBS-GSM-WIFI'
        else:
            positions[client_id]['gps']['method'] = 'LBS-GSM'
        # In some cases dt is empty with value '000000000000': let's avoid that because it'll crash strptime
        if dt:
            positions[client_id]['gps']['datetime'] = dt.astimezone(tz.tzlocal()).strftime('%Y/%m/%d %H:%M:%S')
        else:
            positions[client_id]['gps']['datetime'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        # Special value for 'valid' flag when dt is '000000000000' which may be an invalid position after all
        positions[client_id]['gps']['valid'] = (2 if dt is None else 1)
        positions[client_id]['gps']['nb_sat'] = ''
        # We will need to pad latitude and longitude with + sign if missing
        positions[client_id]['gps']['latitude'] = '{0:{1}}'.format(decoded_position['location']['lat'], '+' if decoded_position['location']['lat'] >= 0 else '')
        positions[client_id]['gps']['longitude'] = '{0:{1}}'.format(decoded_position['location']['lng'], '+' if decoded_position['location']['lng'] >= 0 else '')
        positions[client_id]['gps']['accuracy'] = decoded_position['accuracy']
        positions[client_id]['gps']['speed'] = ''
        positions[client_id]['gps']['heading'] = ''
    await LOGGER('location', 'location_log.txt', addresses[client_id]['address'][0], addresses[client_id].get('imei', 'unknown'), '', positions[client_id]['gps'])

    # And return the second stage of response, which will be sent in the handle_package() function
    # The latitudes and longitudes are truncated to the 6th digit after decimal separator but must preserve the sign
    if positions[client_id]['gps'].get('latitude') and positions[client_id]['gps'].get('longitude'):
        lat_str = positions[client_id]['gps']['latitude'][0] + str(round(float(positions[client_id]['gps']['latitude'][1:]), 6))
        lon_str = positions[client_id]['gps']['longitude'][0] + str(round(float(positions[client_id]['gps']['longitude'][1:]), 6))
        response = '2C'.join([bytes(lat_str, 'UTF-8').hex(), bytes(lon_str, 'UTF-8').hex()])
    else:
        response = ''
    r_2 = make_content_response(hex_dict['start'] + hex_dict['start'], protocol, response, hex_dict['stop_1'] + hex_dict['stop_2'], ignoreDatetimeLength=False, ignoreSeparatorLength=False, forceLengthToValue=0)

    # Send the response corresponding to what is expected by the protocol
    # 0x17 : only send r_1 (returned and handled by send_content_response())
    if (protocol == '17'):
        return(r_1)

    elif (protocol == '69'):
        print(f'[{addresses[client_id]["address"][0]}] OUT Hex : {r_1} (length in bytes = {len(bytes.fromhex(r_1))})')
        await send_response(client_id, r_1)
        return(r_2)


def answer_upload_interval(client_id, query):
    """
    Whenever the device received an SMS that changes the value of an upload interval,
    it sends this information to the server.
    The server should answer with the exact same content to acknowledge the packet.
    """

    # Read protocol
    if len(query) < 4:
        return ''
    protocol = query[1]

    # Response is new upload interval reported by device (HEX formatted, no need to alter it)
    response = ''.join(query[2:4])

    r = make_content_response(hex_dict['start'] + hex_dict['start'], protocol, response, hex_dict['stop_1'] + hex_dict['stop_2'], ignoreDatetimeLength=False, ignoreSeparatorLength=False)
    return(r)


def generic_response(protocol):
    """
    Many queries made by the device do not expect a complex
    response: most of the times, the device expects the exact same packet.
    Here, we will answer with the same value of protocol that the device sent, 
    not using any content.
    """
    r = make_content_response(hex_dict['start'] + hex_dict['start'], protocol, None, hex_dict['stop_1'] + hex_dict['stop_2'], ignoreDatetimeLength=False, ignoreSeparatorLength=False)
    return(r)


def make_content_response(start, protocol, content, stop, ignoreDatetimeLength, ignoreSeparatorLength, forceLengthToValue=None):
    """
    This is just a wrapper to generate the complete response
    to a query, given its content.
    It will apply to all packets where response is of the format:
    start-start-length-protocol-content-stop_1-stop_2.
    Other specific packets where length is replaced by counters
    will be treated separately.

    The forceLengthToValue flag allows bypassing calculation of content length,
    in case the expected response should contain the length that was in the query,
    and not the actual length of the response
    """
    
    # Length is easier that of content (minus some stuff) or fixed to 1, supposedly
    if (forceLengthToValue is None):
        length = (len(bytes.fromhex(content))+1 if content else 1)

        # Length is computed either on the full content or by discarding datetime and separators
        # This is really a wild guess, because documentation is poor...
        if (ignoreDatetimeLength and length >= 6):
            length = length - 6
        # When latitude/longitude are returned, the separator 2C isn't counted in length, apparently
        if (ignoreSeparatorLength and length >= 1):
            length = length - 1

    # Handle case of length forced to a given value
    else:
        length = int(forceLengthToValue)
        
    # Convert length to hexadecimal value
    length = format(length, '02X')

    return(start + length + protocol + (content if content else '') + stop)


async def send_response(client_id, response):
    """
    Function to send a response packet to the client.
    """
    await LOGGER('info', 'server_log.txt', addresses[client_id]['address'][0], addresses[client_id].get('imei', 'unknown'), 'OUT', response)
    writer = addresses[client_id]['writer']
    writer.write(bytes.fromhex(response))
    await writer.drain()


def get_hexified_datetime(truncatedYear):
    """
    Make a fancy function that will return current GMT datetime as hex
    concatenated data, using 2 bytes for year and 1 for the rest.
    The returned string is YY YY MM DD HH MM SS if truncatedYear is False,
    or just YY MM DD HH MM SS if truncatedYear is True.
    """

    # Get current GMT time into a list
    if (truncatedYear):
        dt = datetime.utcnow().strftime('%y-%m-%d-%H-%M-%S').split("-")
    else:
        dt = datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S').split("-")

    # Then convert to hex with 2 bytes for year and 1 for the rest
    dt = [ format(int(x), '0'+str(len(x))+'X') for x in dt ]
    return(''.join(dt))


async def OpenCellID_geolocation_service(api_key, positionDict):
    """
    This wrapper function will query the OpenCellID REST API with the list
    of cell towers identifiers that the device detected.
    It requires an OpenCellID API key.
    
    OpenCellID works by looking up individual cell tower locations from a database.
    We'll use the strongest signal cell tower or average multiple towers if available.
    
    Note: OpenCellID primarily works with cell tower data, not WiFi.
    WiFi geolocation is not supported by OpenCellID.
    
    More information available at https://opencellid.org/
    """
    print(f'OpenCellID Geolocation API queried with: {positionDict}')
    
    if not api_key:
        return {'error': 'OpenCellID API key missing'}
    
    # Get cell tower information
    gsm_cells = positionDict.get('gsm-cells', [])
    gsm_carrier = positionDict.get('gsm-carrier', {})
    mcc = gsm_carrier.get('MCC', 0)
    mnc = gsm_carrier.get('MNC', 0)
    
    if not gsm_cells or mcc == 0 or mnc == 0:
        return {'error': 'No cell tower data available'}
    
    # Try to get location from cell towers
    # Sort by signal strength (strongest first, which is less negative)
    sorted_cells = sorted(gsm_cells, key=lambda x: x.get('signalStrength', -100), reverse=True)
    
    locations = []
    loop = asyncio.get_event_loop()
    
    # Try up to 3 strongest cell towers
    for cell in sorted_cells[:3]:
        try:
            lac = cell.get('locationAreaCode', 0)
            cell_id = cell.get('cellId', 0)
            
            if lac == 0 or cell_id == 0:
                continue
            
            # OpenCellID REST API endpoint
            url = 'https://opencellid.org/cell/get'
            params = {
                'key': api_key,
                'mcc': mcc,
                'mnc': mnc,
                'lac': lac,
                'cellid': cell_id,
                'format': 'json'
            }
            
            # Run the synchronous HTTP request in an executor to avoid blocking
            response = await loop.run_in_executor(
                None,
                lambda: requests.get(url, params=params, timeout=5)
            )
            
            if response.status_code == 200:
                cell_info = response.json()
                # Check if the response indicates success
                if cell_info.get('status') == 'ok' and 'lat' in cell_info and 'lon' in cell_info:
                    locations.append({
                        'lat': float(cell_info['lat']),
                        'lon': float(cell_info['lon']),
                        'accuracy': int(cell_info.get('range', 1000))  # range in meters
                    })
            elif response.status_code == 400:
                # Invalid request - skip this cell
                continue
            else:
                # Rate limit or other error - log and continue
                print(f'OpenCellID API error for cell {cell}: HTTP {response.status_code}')
                continue
                
        except requests.exceptions.RequestException as e:
            print(f'Error querying cell tower {cell}: {e}')
            continue
        except (ValueError, KeyError) as e:
            print(f'Error parsing response for cell tower {cell}: {e}')
            continue
    
    if not locations:
        return {'error': 'No cell tower locations found in OpenCellID database'}
    
    # Average the locations if we have multiple
    if len(locations) == 1:
        result = locations[0]
    else:
        # Calculate weighted average based on accuracy (better accuracy = higher weight)
        total_weight = sum(1.0 / max(loc['accuracy'], 1) for loc in locations)
        lat = sum(loc['lat'] * (1.0 / max(loc['accuracy'], 1)) for loc in locations) / total_weight
        lon = sum(loc['lon'] * (1.0 / max(loc['accuracy'], 1)) for loc in locations) / total_weight
        # Use worst (largest) accuracy as overall accuracy
        accuracy = max(loc['accuracy'] for loc in locations)
        result = {'lat': lat, 'lon': lon, 'accuracy': accuracy}
    
    geoloc = {
        'location': {
            'lat': result['lat'],
            'lng': result['lon']
        },
        'accuracy': result['accuracy']
    }
    
    print(f'OpenCellID Geolocation API returned: {geoloc}')
    return geoloc


async def main():
    """
    Main async function to start the server.
    """
    # Import dotenv with API keys and initialize API connections
    load_dotenv()
    global opencellid_api_key
    opencellid_api_key = os.getenv('OPENCELLID_API_KEY')

    # Details about host server
    HOST = ''
    PORT = 5023

    # Create server
    server = await asyncio.start_server(handle_client, HOST, PORT)
    
    addr = server.sockets[0].getsockname()
    print(f'Server started on {addr[0]}:{addr[1]}')
    print("Waiting for connections...")

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped by user.")

