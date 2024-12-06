import subprocess
import logging

logger = logging.getLogger('__main__.' + __name__)

class Cmd:
    """Run Command Prompt commands in Python"""
    def __init__(self):
        pass

    def run_cmd(self, cmd):
        result = subprocess.run(
            cmd, 
            stdout=subprocess.PIPE,
            # stderr=subprocess.PIPE,
            text=True,
        )
        # stdout = result.stdout
        # stderr = result.stderr
        # logger.info(stdout)
        # logger.info(stderr)

        return result


class CmdError(Exception):
    pass
