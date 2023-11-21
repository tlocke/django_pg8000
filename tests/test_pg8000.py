DATABASES = {
    "default": {
        "ENGINE": "django_pg8000",
        "HOST": "postgres",
        "PORT": 5432,
        "NAME": "django",
        "USER": "postgres",
        "PASSWORD": "postgres",
        "OPTIONS": {},
    },
    "other": {
        "ENGINE": "django_pg8000",
        "HOST": "postgres",
        "PORT": 5432,
        "NAME": "django_other",
        "USER": "postgres",
        "PASSWORD": "postgres",
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
