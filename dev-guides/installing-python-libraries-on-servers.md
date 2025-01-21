# Installing Python Libraries on Servers
Because  [PyPi](https://pypi.org/) and other websites that host python libraries that enable the use of `pip install [library]` aren't whitelisted on the server proxies, we have to install them offline.

## How-to Install a Single Library into the System Offline
1. start on your local machine that has access to the internet
2. open a PowerShell terminal
3. `cd [path/to/dir]`
4. `pip download [library]`
5. copy and paste the downloaded files onto the server
6. `python -m pip install --no-index --find-links=[path/to/dir/with/the/files] [library]`

## How-to Install All Libraries in a requirements.txt File into a Venv Offline
Requirements: you have cloned the GitHub repo for the python program on your local machine
1. start on your local machine that has access to the internet
2. open a PowerShell terminal
3. `cd [path/to/dir/that/contains/requirements.txt]`
4. there should already be a `wheels` folder in the dir, but create one if not
5. `pip download -r requirements.txt -d wheels`
6. push the downloaded files to GitHub
7. go to the server
8. pull the downloaded files from GitHub
9. `cd [path/to/dir/that/contains/requirements.txt]`
10. Activate venv
    * if `.venv` exists, run `.venv\Scripts\Activate.ps1`
    * else, you first need to initialize it by with `python -m venv .venv`
11. `python -m pip install --no-index --find-links=wheels/ -r requirements.txt`

## How-to Upgrade and Install All Libraries in a requirements.txt File into a Venv Offline
Requirements: you have cloned the GitHub repo for the python program on your local machine
1. start on your local machine that has access to the internet and the new version of python that the program will be running on on the server
2. open a PowerShell terminal
3. `cd [path/to/dir/that/contains/requirements.txt]`
4. If you already initialized `.venv` with a different version of python, then delete it and re-initialize it so it's on the new version with `python -m venv .venv`
4. There should already be a `wheels` folder in the dir, but create one if not
5. `pip download -r requirements.txt -d wheels`
6. push the downloaded files to GitHub
7. go to the server
8. pull the downloaded files from GitHub
9. `cd [path/to/dir/that/contains/requirements.txt]`
10. Activate venv
    * if `.venv` exists, run `.venv\Scripts\Activate.ps1`
    * else, you first need to initialize it by with `python -m venv .venv`
11. `python -m pip install --no-index --find-links=wheels/ -r requirements.txt`