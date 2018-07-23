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
