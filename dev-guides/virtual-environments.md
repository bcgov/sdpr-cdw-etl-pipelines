# Python Virtual Environments (venv)

### Why use a venv?
To manage libraries that your program depends on. Without a venv, installing python libraries using `pip install` will install them for the entire system. This can create issues when there are multiple python programs on the system that require different versions of libraries and python. With a venv, libraries can be installed into a .venv folder in the same directory as the the python program that it's built for.

### How to initialize a venv
1. open a powershell terminal
2. run `cd [path to directory you want to install the venv to]`
3. run `python -m venv .venv` to initialize the venv in a hidden folder called .venv

### How to activate and deactivate the venv
* run `.venv\Scripts\Activate.ps1` to activate the venv, assuming you are already in the directory with the `.venv` folder
    * The venv needs to be active for: 
        * `pip install` to install libraries into `.venv\Lib\site-packages` instead of the system
        * the python program that depends on it to have access to the libraries installed within it.
* run `deactivate` to deactivate the venv
    * Now, you will have access only to the libraries that were installed to the system

### How to enable the recreation of the venv (delete + re-build)
With the venv activated, run `pip3 freeze > requirements.txt` in powershell to create a file called requirements.txt that contains the library versions that are installed in the venv. This command can be rerun every time new packages are installed into the venv to update the requirements.txt file. Once created, it enables a developer to initialize and activate a new venv, as previously described, and install all of the required libraries into it by simply running `pip install -r requirements.txt`. This is crucial because venvs are not source code. They do not get pushed to code repositories. Rather, they are built as required on the machines that run the source code that depends on the libraries inside them.