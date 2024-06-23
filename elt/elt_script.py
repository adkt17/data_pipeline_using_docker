import subprocess
import time


def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connections" in result.stdout:
                print("Successfully connected to Postgres")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to Postgres: {e}")
            retries += 1
            print(f"Retrying in {delay_seconds} seconds (Attempt {retries}/{max_retries})")
        time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False


if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT script")

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',  # Replace with actual password
    'host': 'source_postgres'
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',  # Replace with actual password
    'host': 'destination_postgres'
}

dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],  # Correct database name here
    '-f', 'data_dump.sql',
    '-w'
]


subprocess_env = {'PGPASSWORD': source_config['password']}

try:
    result = subprocess.run(dump_command, env=subprocess_env, check=True, capture_output=True, text=True)
    print("Database dump completed successfully")
    print(result.stdout)
    print(f"Standard Error: {result.stderr}")  # Capture and print standard error
except subprocess.CalledProcessError as e:
    print(f"Error during database dump: {e}")
    print(f"stdout: {e.stdout}")
    print(f"stderr: {e.stderr}")
    exit(1)

load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

subprocess_env = {'PGPASSWORD': destination_config['password']}

try:
    result = subprocess.run(load_command, env=subprocess_env, check=True, capture_output=True, text=True)
    print("Database load completed successfully")
    print(result.stdout)
    print(f"Standard Error: {result.stderr}")  # Capture and print standard error
except subprocess.CalledProcessError as e:
    print(f"Error during database load: {e}")
    print(f"stdout: {e.stdout}")
    print(f"stderr: {e.stderr}")
    exit(1)

print("Ending ELT script...")
