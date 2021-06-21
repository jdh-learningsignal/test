For all of the following commands, run them in the directory with ```babel.cfg```, and under the ```venv```. For example:

```
source /var/www/venv/bin/activate
cd /var/www/pysres/sres
```

Run the following to extract strings:

```
pybabel extract -F babel.cfg -o messages.pot .
```

This will generate a ```messages.pot``` file which contains all the strings for translation/localisation.

Then, either make a ```po``` file from the ```pot``` file for the first time, or update an existing one if one exists.

To initialise a new messages.po file for ```en```, run the following:

```
pybabel init -i messages.pot -d translations -l en
```

Alternatively, to update an exisitng messages.po file, run the following:

```
pybabel update -i messages.pot -d translations
```

Then, use a text editor to edit the ```messages.po``` file.

Finally, compile translations for use by running:

```
pybabel compile -d translations
```

Restart SRES.
