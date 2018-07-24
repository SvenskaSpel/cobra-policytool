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


## Make a new release

Right now we do this manually. To be automated later.

1. Bump version number in `setup.py`.
2. Commit `setup.py` and make sure you have no uncommitted changes and push to master.
3. Tag head in master with the same version number, for example v1.0.0 `git tag v1.0.0 ; git push --tags`
4. Build  the distribution `python setup.py sdist bdist_wheel`
5. Upload to PyPi `twine upload dist/*`
