# Upgrading Python on Servers

When a version of python that is currently installed on the servers is required to be upgraded, the new version should be supported by the libraries that python programs running on the servers depend on. I recommend installing the previous version of python rather than the latest one because it sometimes takes a while for libraries to become compatible with the latest version of python.

## An Approach to Upgrading Python Without Breaking Library Dependencies
1. Install the latest subversion of the previous version of python. E.g. if 3.13 is the latest version and 3.12.8 is the latest subversion of the previous version, install 3.12.8.
2. Upgrade all libraries to a version that is compatible with the new version of python. Libraries installed in the system as well as virtual environments will need to be upgraded. Every python program that runs ETL should run inside a virtual enviornment or container. If you recieve an error message saying that there isn't a version of a library that's compatible with the new version of python available, google the latest version of python that the library supports and repeat step (1.) and (2.). If this sounds inconvenient, then you see why we should be focused on breaking down our monolithic ETL architecture into a set of independent components that can be containerized using Docker and deployed on OpenShift.

### What This Looks Like Practically
1. Go to the server
2. Install the new version of python 
    * Add environment variables to the system PATH if necessary
3. Inside each python program, open the `.venv` folder
4. Open `.venv\pyvenv.cfg` to see the current python version used by the program
5. Delete the `.venv` folder
6. Open a PowerShell terminal
7. Run `cd [path/to/dir/to/install/.venv/in]`
8. Make sure the terminal is running the new python version with `python --version`. If it's not, 
    * see the installed versions by running: `py --list-paths`
    * delete the paths to the old python version from the system's PATH environment variable
    * restart both your IDE and terminal
9. Initialize a new `.venv` with `python -m venv .venv` 
    * See the `dev-guides/virtual-environments.md`.
10. Update `.whl` files in the `wheels` folder so you can use them to do an offline install of the required libraries on the server. See `dev-guides/installing-python-libraries-on-servers.md`.




* Note: All python programs that run on the servers should be in a repository for our GitHub Team. 

## A Better Approach in the Future
We want to develop <i>software</i> that can quickly be installed and built on any machine rather than <i>scripts</i> that are bound to messy, monolithic servers. Pairing modern DevOps practices with development makes this simplier than you might assume.
1. Develop independent, portable components--microservices--instead of programs that can't be detached from system-specific resources and services.
2. Containerize these components using Docker.
3. Deploy them on OpenShift instead of servers.
