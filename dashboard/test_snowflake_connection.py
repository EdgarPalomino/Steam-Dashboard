from pathlib import Path
from cryptography.hazmat.primitives import serialization
import snowflake.connector

def test_snowflake_connection(key_path: str):
    try:
        # ---- Load RSA key ----
        key_path_obj = Path(key_path)
        print(f"Loading private key from: {key_path_obj}")

        with open(key_path_obj, "rb") as key_file:
            private_key_obj = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )

        private_key_bytes = private_key_obj.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        print("Private key loaded successfully.")

        # ---- Connect to Snowflake ----
        print("Attempting to connect to Snowflake...")
        conn = snowflake.connector.connect(
            user="CHEETAH",
            account="NMB12256",
            private_key=private_key_bytes,
            warehouse="CHEETAH_WH",
            database="STEAM_ANALYTICS",
            schema="RAW",
        )

        print("Connection established!")

        # ---- Run a test query ----
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        result = cursor.fetchone()

        print(f"Test query successful. Current Snowflake time: {result[0]}")

        cursor.close()
        conn.close()

    except Exception as e:
        print("❌ Failed to connect to Snowflake.")
        print("Error:", e)


if __name__ == "__main__":
    # Set your RSA key path here
    key_path = r"C:\Users\jakek\Documents\Data_Management\rsa_key.p8"
    
    test_snowflake_connection(key_path)
