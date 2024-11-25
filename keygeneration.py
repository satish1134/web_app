import os
from cryptography.fernet import Fernet
from dotenv import set_key

# Generate a new secret key and save it to a file
def generate_secret_key():
    key = Fernet.generate_key()
    with open('secret.key', 'wb') as key_file:
        key_file.write(key)
    return key

# Encrypt a value using the secret key
def encrypt_value(value, key):
    fernet = Fernet(key)
    return fernet.encrypt(value.encode()).decode()

# Set encrypted environment variables in .env file
def create_env_file(encrypted_values):
    with open('.env', 'w') as env_file:
        for key, value in encrypted_values.items():
            env_file.write(f"{key}={value}\n")

def main():
    # Generate secret key and save it to secret.key
    key = generate_secret_key()
    print("Secret key generated and saved to 'secret.key'.")

    # Replace with your DB credentials
    credentials = {
        "HOST": "",
        "PORT": "",
        "USER": "",
        "PASSWORD": "",
        "DATABASE": ""
    }

    # Encrypt each credential
    encrypted_values = {k: encrypt_value(v, key) for k, v in credentials.items()}
    
    # Create .env file with encrypted values
    create_env_file(encrypted_values)
    print(".env file created with encrypted credentials.")

if __name__ == "__main__":
    main()
