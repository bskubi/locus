from typing import *
import paramiko

class ExecCommandResult:
    def __init__(self, stdin, stdout, stderr):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
    
    def read(self, which=Literal["stdin", "stdout", "stderr"]):
        options = {
            "stdin": self.stdin,
            "stdout": self.stdout,
            "stderr": self.stderr
        }
        return options[which].read().decode().strip()

class JumpClient:
    def __init__(self, jump_host, final_host, username: str, jump_password: str, final_password: str):
        # --- Establish connection to the jump server ---
        jump_client = paramiko.SSHClient()
        jump_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        jump_client.connect(
            hostname=jump_host,
            username=username,
            password=jump_password
        )

        # Create a transport channel from the jump server to the final server
        jump_transport = jump_client.get_transport()
        dest_addr = (final_host, 22)
        local_addr = ('127.0.0.1', 22) # Not used, but required
        jump_channel = jump_transport.open_channel("direct-tcpip", dest_addr, local_addr)

        # --- Establish connection to the final server via the channel ---
        final_client = paramiko.SSHClient()
        final_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Use the channel from the jump server as the connection 'socket'
        final_client.connect(
            hostname=final_host,
            username=username,
            password=final_password,
            sock=jump_channel
        )

        self.final_client = final_client
        self.jump_client = jump_client

    def __exit__(self, exc_type, exc_value, traceback):
        self.jump_client.close()
        self.final_client.close()
    
    def exec_command(self, command: str):
        return ExecCommandResult(*self.final_client.exec_command(command))
    
    def channel(self):
        return self.final_client.invoke_shell()