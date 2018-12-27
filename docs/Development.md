# Development

To develop Policytool create a Python virtual environment where following
modules are installed:
* requests
* requests-kerberos
* click

Also add policytool source directory to your python path when using the
REPL. To run the commandline in development mode activate your python 
virtual environment in your shell and do:

```
$ python policytool/cli.py tags_to_atlas --srcdir examples/working/ --environment utv -v -c my-local-config.json
```  

Pushes of branches of automatically build by [Azure pipelines](https://dev.azure.com/SvenskaSpel/cobra-policytool/_build?definitionId=1).

## Make a new release

Right now we do this manually. To be automated later.

1. Bump version number in `setup.py`.
2. Commit `setup.py` and make sure you have no uncommitted changes and push to master.
3. Go to [Releaes](https://github.com/SvenskaSpel/cobra-policytool/releases) on github and click *Draft a new release*
4. Fill in the form. Version tag shall be on the form vX.Y.Z
5. [Azure pipelines](https://dev.azure.com/SvenskaSpel/cobra-policytool/_build?definitionId=2) builds and pushes the release to [PyPi](https://pypi.org/project/cobra-policytool/).
