# Django pg8000

A Django database backend for pg8000

# Installation And Usage

Install with: `pip install django_pg8000`

Set the Django `DATABASES` to something like:

    DATABASES = {
        "default": {
            "ENGINE": "django_pg8000",
            "HOST": "localhost",
            "PORT": 5432,
            "NAME": "django",
            "USER": "postgres",
            "OPTIONS": {},
        },
    }


# Contributing

At the moment Django pg8000 doesn't pass all of the tests, so if you'd like to contribute, please
send a Pull Request that fixes a particular test. 


# Testing

* Git clone Django
* `cd tests`
* `python -m pip install -e ..`
* `python -m pip install -r requirements/py3.txt`
* `./runtests.py databases`

This will run the standard tests against the SQLite backend.

Create a file at `django/django/conf/test_pg8000.py`:

    DATABASES = {
        "default": {
            "ENGINE": "django_pg8000",
            "HOST": "localhost",
            "PORT": 5432,
            "NAME": "django",
            "USER": "postgres",
            "OPTIONS": {},
        },
        "other": {
            "ENGINE": "django_pg8000",
            "HOST": "localhost",
            "PORT": 5432,
            "NAME": "django_other",
            "USER": "postgres",
            "OPTIONS": {},
        },
    }

    SECRET_KEY = "django_tests_secret_key"

    # Use a fast hasher to speed up tests.
    PASSWORD_HASHERS = [
        "django.contrib.auth.hashers.MD5PasswordHasher",
    ]

    DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

    USE_TZ = False

Then run `./runtests.py --failfast --parallel=1 --exclude-tag=psycopg\_specific --settings=django.conf.test\_pg8000`

Some tests in the Django suite are specific to the pyscopg driver, so these can be marked with the
`psycopg\_specific`
[tag](https://docs.djangoproject.com/en/4.2/topics/testing/tools/#topics-tagging-tests).

If tests fail then you may need to drop the databases that are left hanging around by doing something
like:
    psql --username=postgres -c "DROP DATABASE IF EXISTS test_django;" -c "DROP DATABASE IF EXISTS test_django_other;"


# Doing A Release Of Django pg8000

Run ``tox`` to make sure all tests pass, then update the release notes, then do:

::

  git tag -a x.y.z -m "version x.y.z"
  rm -r dist
  python -m build
  twine upload dist/*


# Release Notes

## Version 0.0.1

* Initial release.
