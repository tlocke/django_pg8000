# Django pg8000

A Django database backend for pg8000


# Testing

* Git clone Django
* `cd tests`
* `python -m pip install -e ..`
* `python -m pip install -r requirements/py3.txt`
* `./runtests.py databases`

This will run the standard tests against the SQLite backend.

Create a file at `django/django/conf/test_pg8000.py

Then run `./runtests.py --settings=django.conf.test\_pg8000 backends`
