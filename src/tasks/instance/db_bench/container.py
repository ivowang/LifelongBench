import docker
import mysql.connector
import random
import socket
import time
from docker.models import containers
from typing import Optional


class DBBenchContainer:
    port = 13000
    password = "password"

    def __init__(self, image: str = "mysql:8.0"):
        self.deleted = False
        self.image = image
        self.client = docker.from_env()
        p = DBBenchContainer.port + random.randint(0, 10000)
        while self.is_port_open(p):
            p += random.randint(0, 20)
        self.port = p
        self.container: containers.Container = self.client.containers.run(
            image,
            name=f"mysql_{self.port}",
            environment={
                "MYSQL_ROOT_PASSWORD": self.password,
                "MYSQL_ROOT_HOST": "%",
            },
            ports={"3306": self.port},
            detach=True,
            tty=True,
            stdin_open=True,
            remove=False,  # Keep container for debugging if it fails
            command=["mysqld", "--default-authentication-plugin=mysql_native_password"],
        )

        # Wait for MySQL container to be ready
        max_retries = 60  # Maximum 60 retries (about 2 minutes)
        retry = 0
        while retry < max_retries:
            try:
                # Check if container is running (handle case where container might be removed)
                try:
                    self.container.reload()
                    container_status = self.container.status
                except Exception as reload_error:
                    # Container might have been removed or doesn't exist
                    if "No such container" in str(reload_error) or "404" in str(reload_error):
                        raise RuntimeError(f"MySQL container was removed unexpectedly. This usually means the container failed to start. Check Docker logs.")
                    raise
                
                if container_status != 'running':
                    if container_status == 'exited':
                        # Container exited, get logs and raise error immediately
                        try:
                            logs = self.container.logs(tail=100).decode('utf-8')
                            exit_code = self.container.attrs.get('State', {}).get('ExitCode', 'unknown')
                            print(f"MySQL container exited with code {exit_code}. Container logs:\n{logs}")
                        except Exception as log_error:
                            print(f"Could not retrieve container logs: {log_error}")
                        raise RuntimeError(f"MySQL container exited immediately after start. Check logs above for details.")
                    
                    if retry % 10 == 0:  # Log every 10 retries
                        print(f"Waiting for MySQL container to start... (status: {container_status})")
                    time.sleep(2)
                    retry += 1
                    continue
                
                # Try to connect
                self.conn = mysql.connector.connect(
                    host="127.0.0.1",
                    user="root",
                    password=self.password,
                    port=self.port,
                    pool_reset_session=True,
                    auth_plugin='mysql_native_password',
                    connection_timeout=5,
                )
                break  # Connection successful
            except mysql.connector.errors.OperationalError as e:
                if retry % 10 == 0:  # Log every 10 retries
                    print(f"MySQL not ready yet, retrying... (attempt {retry + 1}/{max_retries})")
                time.sleep(2)
                retry += 1
            except mysql.connector.InterfaceError as e:
                if retry > 10:
                    raise
                time.sleep(5)
                retry += 1
            except RuntimeError:
                # Re-raise RuntimeError (container removed)
                raise
            except Exception as e:
                if retry % 10 == 0:
                    print(f"Unexpected error connecting to MySQL: {e}")
                time.sleep(2)
                retry += 1
        
        if retry >= max_retries:
            # Get container logs for debugging
            try:
                logs = self.container.logs(tail=50).decode('utf-8')
                print(f"MySQL container logs:\n{logs}")
            except Exception as log_error:
                print(f"Could not retrieve container logs: {log_error}")
            raise RuntimeError(f"Failed to connect to MySQL container after {max_retries} retries")

    def delete(self) -> None:
        try:
            if self.container:
                try:
                    self.container.reload()
                    if self.container.status == 'running':
                        self.container.stop()
                except Exception:
                    # Container might already be stopped or removed
                    pass
                try:
                    self.container.remove()
                except Exception:
                    # Container might already be removed
                    pass
        except Exception:
            # Ignore errors during cleanup
            pass
        self.deleted = True

    def __del__(self) -> None:
        try:
            if not self.deleted:
                self.delete()
        except Exception:  # noqa
            pass

    def execute(
        self,
        multiple_sql: str,
        database: Optional[str] = None,
    ) -> str:
        self.conn.reconnect()
        try:
            cursor = self.conn.cursor()
            if database:
                cursor.execute(f"use `{database}`;")
                cursor.fetchall()
            sql_list = multiple_sql.split(";")
            sql_list = [sql.strip() for sql in sql_list if sql.strip() != ""]
            result = ""
            for sql in sql_list:
                cursor.execute(sql)
                result = str(cursor.fetchall())
                self.conn.commit()
        except Exception as e:
            result = str(e)
        return result

    def is_port_open(
        self, port: int
    ) -> bool:  # noqa (The quality checker of the IDE is wrong)
        try:
            self.client.containers.get(f"mysql_{port}")
            return True
        except Exception:  # noqa
            pass

        # Create a socket object
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # use IPv4 and TCP
        try:
            # Try to connect to the specified port
            sock.connect(("localhost", port))
            # If the connection succeeds, the port is occupied
            return True
        except ConnectionRefusedError:
            # If the connection is refused, the port is not occupied
            return False
        finally:
            # Close the socket
            sock.close()
