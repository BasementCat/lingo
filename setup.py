import os

from setuptools import setup

# Using vbox, hard links do not work
if os.environ.get('USER','') == 'vagrant':
    del os.link

setup(
	name='lingo',
	version='0.8',
	description='A simple MongoDB/CouchDB ORM for Python',
	long_description='',
	classifiers=[
		'Development Status :: 3 - Alpha',
		'Intended Audience :: Developers',
		'License :: Other/Proprietary License',
		'Natural Language :: English',
		'Operating System :: OS Independent',
		'Programming Language :: Python :: 2',
		'Topic :: Database',
		'Topic :: Software Development :: Libraries'
	],
	keywords='mongo mongodb orm python couchdb',
	url='https://github.com/BasementCat/lingo',
	author='BasementCat',
	author_email='basementcat@basementcat.net',
	license='',
	packages=['lingo'],
	install_requires=['pymongo', 'couchdb', 'python-dateutil', 'pytz'],
	test_suite='nose.collector',
	tests_require=['nose'],
	zip_safe=False
)