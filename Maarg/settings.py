"""
Django settings for Maarg project.

Generated by 'django-admin startproject' using Django 4.0.6.

For more information on this file, see
https://docs.djangoproject.com/en/4.0/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/4.0/ref/settings/
"""

from pathlib import Path
import os

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

PROD = True

if PROD == True:
    OSM_DATA_DIR = '/home/bender/routeoptimization/osmfiles/'
    OSM_CONFIG_JSON_PATH = '/home/bender/routeoptimization/openrouteservice/docker/conf/ors-config.json'
    CONTAINER_ID = '4e39a50b616a'
else:
    OSM_DATA_DIR = "D:/code/ascent/route optimization/Engines/ors/openrouteservice/docker/data"
    OSM_CONFIG_JSON_PATH = "D:/code/ascent/route optimization/Engines/ors/openrouteservice/docker/conf/ors-config.json"
    CONTAINER_ID = '899da61832ec'

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-$i30iqu@y_c51ei4ej@_slqk2lmt+6#0msdw(5h!5u(k^un1h0'

# SECURITY WARNING: don't run with debug turned on in production!
if PROD == False:
    DEBUG = True
else:
    DEBUG = True

ALLOWED_HOSTS = ['localhost', '127.0.0.1', '10.150.0.24']


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'vyuha.apps.VyuhaConfig'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'Maarg.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages'
            ],
        },
    },
]

WSGI_APPLICATION = 'Maarg.wsgi.application'


# Database
# https://docs.djangoproject.com/en/4.0/ref/settings/#databases


if PROD == True:
    DATABASES = {
        'default': {
            #'ENGINE': 'django.db.backends.sqlite3',
            #'NAME': BASE_DIR / 'db.sqlite3',
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'maarg',
            'USER': 'rakesh',
            'PASSWORD': 'rakesh_admin',
            'HOST': 'localhost',
            'PORT': '',
        }
    }
else:
    DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
        }
    }

# Password validation
# https://docs.djangoproject.com/en/4.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.0/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.0/howto/static-files/

STATIC_URL = 'static/'

# Default primary key field type
# https://docs.djangoproject.com/en/4.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


#added Manually
# STATICFILES_DIRS = [
#     BASE_DIR / "static"
# ]

STATIC_ROOT = os.path.join(BASE_DIR, 'static/')