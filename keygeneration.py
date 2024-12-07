import os
from cryptography.fernet import Fernet
from dotenv import set_key

def generate_secret_key():
    key = Fernet.generate_key()
    with open('secret.key', 'wb') as key_file:
        key_file.write(key)
    return key

def encrypt_value(value, key):
    if not isinstance(value, str):
        value = str(value)  # Convert non-string values to string
    fernet = Fernet(key)
    return fernet.encrypt(value.encode()).decode()

def create_env_file(encrypted_values):
    # Create or overwrite the .env file
    with open('.env', 'w') as env_file:
        for key, value in encrypted_values.items():
            if value:  # Only write if value is not empty
                env_file.write(f"{key}={value}\n")

def main():
    # Generate secret key and save it to secret.key
    key = generate_secret_key()
    print("Secret key generated and saved to 'secret.key'.")

    # Replace these with your actual Vertica credentials
    credentials = {
        "HOST": "localhost",  # e.g., "localhost" or IP address
        "PORT": "5433",               # Vertica default port
        "USER": "dbadmin",           # Your Vertica username
        "PASSWORD": "your_password",  # Your Vertica password
        "DATABASE": "Vmart"          # Your database name
    }

    # Encrypt each credential
    encrypted_values = {}
    for k, v in credentials.items():
        if v:  # Only encrypt if value is not empty
            encrypted_values[k] = encrypt_value(v, key)
    
    # Create .env file with encrypted values
    create_env_file(encrypted_values)
    print("Credentials encrypted and saved to .env file:")
    for k in credentials.keys():
        print(f"{k}: {'[SET]' if k in encrypted_values else '[NOT SET]'}")

if __name__ == "__main__":
    main()
