import socket
import time
import binascii
import sys
import os

# Ensure we can import gt06_msg from the same directory 
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from gt06_msg import T01, T12, T13

def send_message(sock, msg_obj, name):
    try:
        msg_no, msg_bytes = msg_obj.get_msg()
        if msg_no == -1:
            print(f"Failed to generate message for {name}")
            return
            
        print(f"Sending {name} (MsgNo: {msg_no}): {binascii.hexlify(msg_bytes).decode()}")
        sock.send(msg_bytes)
    except Exception as e:
        print(f"Error sending {name}: {e}")

def main():
    host = '127.0.0.1'
    port = 5023
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        print(f"Connected to {host}:{port}")
        
        # 1. Login (T01)
        print("\n--- Sending Login (T01) ---")
        login_msg = T01()
        # IMEI: 15 digits usually, padded to 16 chars (8 bytes)
        login_msg.set_imei("865067040166345") 
        send_message(sock, login_msg, "Login (T01)")
        
        time.sleep(1)

        # 2. Heartbeat (T13)
        print("\n--- Sending Heartbeat (T13) ---")
        heartbeat_msg = T13()
        # set_device_status(defend, acc, charge, alarm, gps, power, voltage_level, gsm_signal)
        heartbeat_msg.set_device_status(
            defend=0, 
            acc=1, 
            charge=1, 
            alarm=0, 
            gps=1, 
            power=1, 
            voltage_level=3, 
            gsm_signal=4
        )
        send_message(sock, heartbeat_msg, "Heartbeat (T13)")
        
        time.sleep(1)

        # 3. GPS Report (T12)
        print("\n--- Sending GPS Report (T12) ---")
        gps_msg = T12()
        # set_gps(date_time, satellite_num, latitude, longitude, speed, course, lat_ns, lon_ew, gps_onoff, is_real_time)
        # date_time: YYMMDDHHmmss
        gps_msg.set_gps(
            date_time="230101120000", 
            satellite_num=8, 
            latitude=22.54321, 
            longitude=113.98765, 
            speed=30, 
            course=180, 
            lat_ns=1, # North
            lon_ew=0, # East
            gps_onoff=1, 
            is_real_time=0
        )
        # set_lbs(mcc, mnc, lac, cell_id)
        gps_msg.set_lbs(mcc=460, mnc=0, lac=1000, cell_id=12345)
        send_message(sock, gps_msg, "GPS Report (T12)")

        # Read responses
        print("\n--- Listening for responses ---")
        sock.settimeout(2.0)
        start_time = time.time()
        while time.time() - start_time < 5: # Listen for up to 5 seconds
            try:
                data = sock.recv(1024)
                if not data:
                    print("Connection closed by server")
                    break
                print(f"Received: {binascii.hexlify(data).decode()}")
            except socket.timeout:
                break
            except Exception as e:
                print(f"Error receiving data: {e}")
                break
            
        sock.close()
        print("\nDone.")
        
    except ConnectionRefusedError:
        print(f"Could not connect to {host}:{port}. Is the server running?")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
